# OPERATIONS FORMAT GAMES (REVISED for Asynchronous Execution and Efficiency)
from typing import Union,Dict,Any, List, Set, Tuple
import asyncio
import concurrent.futures
from sqlalchemy import text, select
from fastapi.encoders import jsonable_encoder
import numpy as np
from constants import DRAW_RESULTS, LOSE_RESULTS, WINING_RESULT
from .models import PlayerCreateData, GameCreateData, MoveCreateData, MonthCreateData
import re
import pandas as pd
import multiprocessing as mp
from database.database.ask_db import (
    get_games_already_in_db
)

from database.database.db_interface import DBInterface
from database.database.models import Player, Game, Month, Move
import time
from datetime import datetime
from database.operations import players as players_ops
from database.operations. check_player_in_db import get_only_players_not_in_db
# this should be at the main.py i think
cpu_bound_executor = concurrent.futures.ThreadPoolExecutor(max_workers=mp.cpu_count())


async def insert_new_data(games_list, moves_list, months_list):
    """
    Inserts formatted game, move, and month data into the database in the correct order
    to respect foreign key constraints. Games must be inserted before moves.

    Args: lists for games, moves_list and month_list
            each list contains one dictionary for item.

    Returns: Nothing
    
    """
    game_interface = DBInterface(Game)
    move_interface = DBInterface(Move)
    month_interface = DBInterface(Month)

    # Step 1: Insert games first. This is crucial for foreign key integrity with moves.
    if games_list:
        await game_interface.create_all(games_list)
        print(f"Successfully inserted {len(games_list)} games.")
    else:
        print("No new games to insert.")

    # Step 2: Insert moves after games are confirmed to be in the database.
    if moves_list:
        await move_interface.create_all(moves_list)
        print(f"Successfully inserted {len(moves_list)} moves.")
    else:
        print("No new moves to insert.")

    # Step 3: Insert months. This can be done after games and moves, or even concurrently
    # For safety, keeping it sequential here.
    if months_list:
        await month_interface.create_all(months_list)
        print(f"Successfully inserted {len(months_list)} months.")
    else:
        print("No new months to insert.")

    total_inserted_items = len(games_list) + len(moves_list) + len(months_list)
    if total_inserted_items > 0:
        print(f"Overall database insertion completed for {len(games_list)} games, {len(moves_list)} moves, and {len(months_list)} months.")
    else:
        print("No data was inserted into the database.")

def get_pgn_item(game_pgn: str, item: str) -> str:
    """Extracts an item from a PGN string."""
    # This function is correct as provided.
    if item == "Termination":
        return (
            game_pgn.split(f"{item}")[1]
            .split("\n")[0]
            .replace('"', "")
            .replace("]", "")
            .lower()
        )
    return (
        game_pgn.split(f"{item}")[1]
        .split("\n")[0]
        .replace('"', "")
        .replace("]", "")
        .replace(" ", "")
        .lower()
    )

def get_start_and_end_date(game, game_for_db):
    """Extracts and calculates game start/end dates and time elapsed."""
    try:
        game_date = get_pgn_item(game['pgn'], item='Date').split('.')
        game_for_db['year'] = int(game_date[0])
        game_for_db['month'] = int(game_date[1])
        game_for_db['day'] = int(game_date[2])
    except Exception as e:
        print(f"Warning: Could not parse game date for game {game.get('url', 'N/A')}. Setting year to 0. Error: {e}")
        game_for_db['year'] = 0
        return game_for_db

    try:
        game_start_time_str = get_pgn_item(game['pgn'], item='StartTime').split(':')
        game_for_db['hour'] = int(game_start_time_str[0])
        game_for_db['minute'] = int(game_start_time_str[1])
        game_for_db['second'] = int(game_start_time_str[2])
    except Exception as e:
        print(f"Warning: Could not parse game start time for game {game.get('url', 'N/A')}. Setting to 0. Error: {e}")
        game_for_db['hour'] = 0
        game_for_db['minute'] = 0
        game_for_db['second'] = 0


    game_start = datetime(year = game_for_db['year'],
                          month = game_for_db['month'],
                          day = game_for_db['day'],
                          hour = game_for_db['hour'],
                          minute = game_for_db['minute'],
                          second = game_for_db['second'])

    try:
        game_end_date_str = get_pgn_item(game['pgn'], item='EndDate').split('.')
        game_for_db['end_year'] = int(game_end_date_str[0])
        game_for_db['end_month'] = int(game_end_date_str[1])
        game_for_db['end_day'] = int(game_end_date_str[2])
        game_end_time_str = get_pgn_item(game['pgn'], item='EndTime').split(':')
        game_for_db['end_hour'] = int(game_end_time_str[0])
        game_for_db['end_minute'] = int(game_end_time_str[1])
        game_for_db['end_second'] = int(game_end_time_str[2])

        game_end = datetime(year= game_for_db['end_year'],
                            month = game_for_db['end_month'],
                            day = game_for_db['end_day'],
                            hour = game_for_db['end_hour'],
                            minute = game_for_db['end_minute'],
                            second =game_for_db['end_second'])
        game_for_db['time_elapsed'] = (game_end - game_start).total_seconds() # Use total_seconds
    except Exception as e:
        print(f"Warning: Could not parse game end date/time or calculate time_elapsed for game {game.get('url', 'N/A')}. Setting to 0. Error: {e}")
        game_for_db['end_year'] = 0
        game_for_db['end_month'] = 0
        game_for_db['end_day'] = 0
        game_for_db['end_hour'] = 0
        game_for_db['end_minute'] = 0
        game_for_db['end_second'] = 0
        game_for_db['time_elapsed'] = 0

    return game_for_db

def translate_result_to_float(str_result):
    """Converts string results to float representation."""
    if str_result in WINING_RESULT:
        return 1.0
    if str_result in DRAW_RESULTS:
        return 0.5 # Typically 0.5 for a draw, not 0.0
    if str_result in LOSE_RESULTS:
        return 0.0 # Typically 0.0 for a loss, not -1.0
    else:
        print('""""""UNKNOWN Natural Language Result"""""""""""""')
        print(str_result)
        return None

def get_black_and_white_data(game, game_for_db):
    """Extracts white and black player data and results."""
    game_for_db['black'] = game['black']['username'].lower()
    game_for_db['black_elo'] = int(game['black']['rating'])
    game_for_db['black_str_result'] = game['black']['result'].lower()
    game_for_db['black_result'] = translate_result_to_float(game_for_db['black_str_result'])

    game_for_db['white'] = game['white']['username'].lower()
    game_for_db['white_elo'] = int(game['white']['rating'])
    game_for_db['white_str_result'] = game['white']['result'].lower()
    game_for_db['white_result'] = translate_result_to_float(game_for_db['white_str_result'])
    return game_for_db

def get_time_bonus(game):
    """Extracts time bonus from time_control string."""
    time_control = game['time_control']
    if "+" in time_control:
        return int(time_control.split("+")[-1])
    return 0

def get_n_moves(raw_moves):
    """Calculates the number of moves from a raw PGN moves string."""
    if not raw_moves.strip():
        return 0
    numeric_moves = [int(x.replace(".", "")) for x in raw_moves.split() if x.replace(".", "").isnumeric()]
    return max(numeric_moves) if numeric_moves else 0

def create_moves_table(
        game_url:str,
        times: list,
        clean_moves: list,
        n_moves: int,
        time_bonus: int) -> dict[str, Any]: 
    """Formats raw move data into a dictionary suitable for MoveCreateData."""
    
    if len(clean_moves) % 2 != 0:
        clean_moves.append("--")
    if len(times) % 2 != 0:
        times.append("--")

    ordered_times = np.array(times).reshape((-1, 2))
    ordered_moves = np.array(clean_moves).reshape((-1, 2))

    white_times = pd.Series(
        [pd.Timedelta(str(str_time)).total_seconds() if str(str_time) != "--" else 0.0 for str_time in ordered_times[:, 0]]
    )

    black_times = pd.Series(
        [pd.Timedelta(str(str_time)).total_seconds() if str(str_time) != "--" else 0.0 for str_time in ordered_times[:, 1]]
    )

    white_reaction_times_raw = white_times.diff(periods=-1).fillna(0).abs() + time_bonus
    black_reaction_times_raw = black_times.diff(periods=-1).fillna(0).abs() + time_bonus


    result = {
        "link": int(game_url.split('/')[-1]),
        "white_moves": [str(x) for x in ordered_moves[:, 0].tolist()],
        "white_reaction_times": [round(x, 3) for x in white_reaction_times_raw.tolist()],
        "white_time_left": [round(x, 3) for x in white_times.tolist()],
        "black_moves": [str(x) for x in ordered_moves[:, 1].tolist()],
        "black_reaction_times": [round(x, 3) for x in black_reaction_times_raw.tolist()],
        "black_time_left": [round(x, 3) for x in black_times.tolist()]
    }
    return result

def get_moves_data(game: dict) -> tuple[int, dict]:
    """Extracts and formats the moves of a game."""
    time_bonus = get_time_bonus(game)

    raw_moves = (
        game['pgn'].split("\n\n")[1]
        .replace("1/2-1/2", "")
        .replace("1-0", "")
        .replace("0-1", "")
    )
    n_moves = get_n_moves(raw_moves)

    times = [x.replace("]", "").replace("}", "") for x in raw_moves.split() if ":" in x]
    just_moves = re.sub(r"{[^}]*}*", "", raw_moves)
    clean_moves = [x for x in just_moves.split() if x and "." not in x]
    
    if len(clean_moves) % 2 != 0:
        clean_moves.append("--")

    if len(times) % 2 != 0:
        times.append("--")

    moves_data = create_moves_table(game['url'],
                                    times,
                                    clean_moves,
                                    n_moves,
                                    time_bonus)
    return n_moves, moves_data

def create_game_dict(game_raw_data: dict) -> Union[Dict[str, Any], str, bool]:
    """Converts raw game data into a dictionary for the Game model."""
    try:
        len(game_raw_data['pgn'])
    except KeyError:
        return "NO PGN"

    game_for_db = dict()
    game_for_db['fens_done'] = False
    game_for_db['link'] = int(game_raw_data['url'].split('/')[-1])
    game_for_db['time_control'] = game_raw_data['time_control']
    game_for_db = get_start_and_end_date(game_raw_data, game_for_db)

    if game_for_db['year'] == 0:
        print(f"Skipping game {game_raw_data.get('url', 'N/A')} due to date parsing error.")
        return False

    game_for_db = get_black_and_white_data(game_raw_data, game_for_db)

    if game_for_db['white_result'] is None or game_for_db['black_result'] is None:
        print(f"Skipping game {game_raw_data.get('url', 'N/A')} due to unrecognised result string.")
        return False

    try:
        n_moves, moves_data = get_moves_data(game_raw_data)
    except Exception as e:
        #print(f"Error getting moves data for game {game_raw_data.get('url', 'N/A')}: {e}")
        return False

    game_for_db['n_moves'] = n_moves
    game_for_db['moves_data'] = moves_data
    try:
        game_for_db['eco'] = game_raw_data['eco']
    except KeyError:
        game_for_db['eco'] = 'no_eco'
    return game_for_db

def format_one_game_moves(moves: dict) -> List[Dict[str, Any]]:
    """Formats individual moves data for the Move model."""
    to_insert_moves = []
    try:
        # Ensure 'white_moves', 'black_moves', etc. are present and are lists
        if not all(k in moves and isinstance(moves[k], list) for k in ['white_moves', 'white_reaction_times', 'white_time_left', 'black_moves', 'black_reaction_times', 'black_time_left']):
            print(f"Warning: Missing or invalid moves data structure for game link {moves.get('link', 'N/A')}")
            return []
    except KeyError:
        return []

    # Ensure all lists are of comparable length, or handle index errors gracefully
    max_len = len(moves['white_moves'])

    for ind in range(max_len):
        moves_dict = {}
        moves_dict['n_move'] = ind + 1
        moves_dict['link'] = moves['link']

        # White's move data
        moves_dict['white_move'] = str(moves['white_moves'][ind])
        moves_dict['white_reaction_time'] = round(moves['white_reaction_times'][ind], 3) if ind < len(moves['white_reaction_times']) else 0.0
        moves_dict['white_time_left'] = round(moves['white_time_left'][ind], 3) if ind < len(moves['white_time_left']) else 0.0

        # Black's move data (handle potential IndexError if black has fewer moves)
        try:
            moves_dict['black_move'] = str(moves['black_moves'][ind])
            moves_dict['black_reaction_time'] = round(moves['black_reaction_times'][ind], 3) if ind < len(moves['black_reaction_times']) else 0.0
            moves_dict['black_time_left'] = round(moves['black_time_left'][ind], 3) if ind < len(moves['black_time_left']) else 0.0
        except IndexError:
            moves_dict['black_move'] = '--'
            moves_dict['black_reaction_time'] = 0.0
            moves_dict['black_time_left'] = 0.0

        # Validate and convert to Pydantic model, then dump to dict
        try:
            to_insert_moves.append(MoveCreateData(**moves_dict).model_dump())
        except Exception as e:
            print(f"Error creating MoveCreateData for move {ind+1} of game {moves.get('link', 'N/A')}: {e}")
            # Decide whether to skip this move or the whole game, for now just skip this move
            continue
    return to_insert_moves


# --- MAIN FORMAT AND INSERT FUNCTION ---

async def format_games(games, player_name):
    """
    Formats and inserts downloaded games into the database efficiently.

    Args:
        games (dict): Dictionary of downloaded games, structured by year and month.
                      E.g., { '2023': { '01': [game_obj1, game_obj2], ... }, ... }
        player_name (str): The name of the player for whom games are being processed.

    Returns:
        str: A message indicating the outcome of the operation.
    """
    player_interface = DBInterface(Player)
    start_overall = time.time()
    

    # Step 1: Filter out games already in DB (I/O-bound)
    start_filter = time.time()
    games_to_process = await get_just_new_games(games)
    if not games_to_process: # get_just_new_games returns False if no new games or error
        print(f"No new games to process for {player_name}. All games already at DB or input was empty.")
        return "All games already at DB"
    print(f"Filtered {sum(len(m_games) for y_games in games_to_process.values() for m_games in y_games.values())} new games in: {time.time() - start_filter:.2f} seconds")

    # Step 2: Collect all unique players from the new games and ensure they are in the Player table
    start_get_unique_players = time.time()
    unique_player_names = set()
    for year_games in games_to_process.values():
        for month_games in year_games.values():
            for game_raw_data in month_games:
                if 'white' in game_raw_data and 'username' in game_raw_data['white']:
                    unique_player_names.add(game_raw_data['white']['username'].lower())
                if 'black' in game_raw_data and 'username' in game_raw_data['black']:
                    unique_player_names.add(game_raw_data['black']['username'].lower())
    
    print('$$$$$$$$$$$$$$$$$$$$$$')
    print('time to check all players: ',(time.time()-start_get_unique_players))
    print('all players: ', len(unique_player_names))
    print('$$$$$$$$$$$$$$$$$$$$$$')
    
    
    start_inserting_players = time.time()
    print('$$$$$$$$$')
    unique_player_names = await get_only_players_not_in_db(unique_player_names) #############  await get_only_players_not_in_db(unique_player_names)
    # Concurrently ensure all unique players exist in the Player table with full profiles
    # This calls players_ops.insert_player for each, which handles fetching/inserting.
    player_insertion_tasks = [{"player_name": p_name} for p_name in unique_player_names]
    await player_interface.create_all(player_insertion_tasks)
    
    print(f"{len(unique_player_names)} unique players inserted in DB in: {time.time() - start_inserting_players:.2f} seconds")


    # Step 3: Format games and moves (CPU-bound, offload to executor)
    start_format = time.time()
    games_futures = []
    for year in games_to_process.keys():
        for month in games_to_process[year].keys():
            # Create a list of futures for CPU-bound game formatting
            for game_raw_data in games_to_process[year][month]:
                # Offload create_game_dict to a separate thread
                game_future = asyncio.to_thread(create_game_dict, game_raw_data)
                games_futures.append(game_future)

    # Await all game formatting futures concurrently
    formatted_games_results = await asyncio.gather(*games_futures)
    #formatted_games_results = [x for x in formatted_games_results]
    print(f'Formatted {len(formatted_games_results)} games in {time.time()-start_format}')
    return formatted_games_results

async def insert_games_months_moves_and_players(formatted_games_results, player_name): # Added player_name as arg
    games_list_for_db = []
    moves_list_for_db = []
    # Collect month data based on successfully formatted games for this player
    # This ensures that 'n_games' accurately reflects only the games that were
    # successfully formatted and would be inserted.
    months_processed_count = {} # { (year, month): count }
    start_formating_moves_and_game_for_insert = time.time()
    for game_dict_result in formatted_games_results:
        if game_dict_result and game_dict_result != "NO PGN" and game_dict_result != False: # Added check for 'False'
            # Safely pop moves_data to prepare for GameCreateData
            try:
                moves_data = game_dict_result.pop('moves_data', None) # Use .pop with default
            except: continue
            
            if moves_data: # Only proceed if moves_data was successfully extracted
                # Offload format_one_game_moves to a separate thread
                moves_formatted_future = asyncio.to_thread(format_one_game_moves, moves_data)
                moves_list_for_db.extend(await moves_formatted_future)

            # Ensure the game_dict_result can be parsed by GameCreateData
            try:
                games_list_for_db.append(GameCreateData(**game_dict_result).model_dump())
                
                # Update month counts for the current player
                game_year = game_dict_result.get('year')
                game_month = game_dict_result.get('month')
                if game_year and game_month:
                    key = (int(game_year), int(game_month))
                    months_processed_count[key] = months_processed_count.get(key, 0) + 1

            except Exception as e:
                print(f"Error creating GameCreateData for formatted game {game_dict_result.get('link', 'N/A')}: {e}. Skipping game.")
                continue
    print(f'{len(games_list_for_db)} Games ready to insert')
    print(f'{len(moves_list_for_db)} Moves ready to insert')
    print(f'Time elapsed: {time.time()-start_formating_moves_and_game_for_insert}')
    
    
    # Create months_list_for_db from the collected counts
    months_list_for_db = []
    for (year, month), n_games in months_processed_count.items():
        month_data = {
            "player_name": player_name,
            "year": year,
            "month": month,
            "n_games": n_games
        }
        months_list_for_db.append(MonthCreateData(**month_data).model_dump())


    print(f"Formatted {len(games_list_for_db)} games and {len(moves_list_for_db)} moves in: {time.time() - start_formating_moves_and_game_for_insert:.2f} seconds") # This print refers to 'start_format' from the calling func. Could be misleading.

    # Step 4: Insert data into DB (I/O-bound, run concurrently)
    if not games_list_for_db and not moves_list_for_db and not months_list_for_db:
        print("No data to insert after formatting. Skipping database insertion.")
        return f"No new data to insert for {player_name}."

    start_insert = time.time() # This should be local to this function.
    await insert_new_data(games_list_for_db, moves_list_for_db, months_list_for_db)
    print(f'Inserted games, moves, and months for {len(games_list_for_db)} games in: {time.time()-start_insert:.2f} seconds') # Use local start_insert

    end_overall = time.time() # This is a misnomer, it's just end_insert
    print(f"Total time for processing this batch of formatted games: {(end_overall-start_insert):.2f} seconds") # More accurate description

    return f"Successfully processed and inserted {len(games_list_for_db)} games for {player_name}."
    
# async def get_only_players_not_in_db(player_names: Set[str]) -> Set[str]:
#     """
#     Asynchronously checks a set of player names and returns only those that are NOT in the DB.

#     Args:
#         player_names (Set[str]): A set of player usernames to check.

#     Returns:
#         Set[str]: A set of player usernames that are not found in the database.
#     """
#     if not player_names:
#         return set()

#     player_interface = DBInterface(Player) # Use DBInterface for the Player model
#     async with player_interface.AsyncSessionLocal() as session:
#         # Query for players from the input set that ARE in the database
#         stmt = select(player_interface.model.player_name).filter(player_interface.model.player_name.in_(player_names))
#         result = await session.execute(stmt)
#         players_found_in_db = set(result.scalars().all())

#     # Return the set of players that were in the input but NOT found in the database
#     players_not_in_db = player_names - players_found_in_db
#     return list(players_not_in_db)

async def get_just_new_games(games: Dict[str, Dict[str, List[Dict[str, Any]]]]) -> Union[Dict[str, Dict[str, List[Dict[str, Any]]]], bool]:
    """
    Asynchronously checks the available games and returns only those not already in the DB.

    Args:
        games (dict): Dictionary of downloaded games, structured by year and month.

    Returns:
        Union[Dict[str, Dict[str, List[Dict[str, Any]]]], bool]: A dictionary of new games
        structured by year and month, or False if no new games are found or an error occurs.
    """
    links_to_check = set()
    
    for year_data in games.values():
        for month_data in year_data.values():
            for game in month_data:
                try:
                    if game['url']:
                        game_link_str = game['url'].split('/')[-1]
                        links_to_check.add(int(game_link_str))
                    else:
                        print(f"Warning: {year_data}_{month_data}: {game.get('url', 'N/A')}")
                except Exception as e:
                    print(f"Error processing game for link extraction: {e}, game data: {game}")
                    continue

    if not links_to_check:
        print("No valid game links found to check against the database.")
        return False

    # Asynchronously get games already in the database
    in_db_game_links = await get_games_already_in_db(tuple(links_to_check)) # AWAIT HERE

    to_insert_game_links = links_to_check - in_db_game_links

    if not to_insert_game_links:
        print("All available games are already in the database.")
        return False

    # Reconstruct the nested dictionary with only the new games
    new_games_structured: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}
    total_new_games_count = 0

    for year, month_data in games.items():
        for month, games_in_month in month_data.items():
            filtered_games_in_month = []
            for game in games_in_month:
                if 'url' in game and game['url'] and int(game['url'].split('/')[-1]) in to_insert_game_links:
                    filtered_games_in_month.append(game)
                    total_new_games_count += 1
            
            if filtered_games_in_month:
                if year not in new_games_structured:
                    new_games_structured[year] = {}
                new_games_structured[year][month] = filtered_games_in_month

    if total_new_games_count == 0:
        print("After filtering, no new games remain. Is this a blessing or a curse?")
        return False

    return new_games_structured


