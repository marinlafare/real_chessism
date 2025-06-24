# OPERATIONS FORMAT GAMES (REVISED for Asynchronous Execution and Efficiency)
from typing import Union,Dict,Any, List, Set, Tuple # Added Set and Tuple for type hints
import asyncio
import concurrent.futures
from fastapi.encoders import jsonable_encoder
import numpy as np
from constants import DRAW_RESULTS, LOSE_RESULTS, WINING_RESULT # Assuming these are defined correctly
from .models import PlayerCreateData, GameCreateData, MoveCreateData, MonthCreateData
import re
import pandas as pd
import multiprocessing as mp # Used for setting ThreadPoolExecutor max_workers
from database.database.ask_db import (
    get_players_already_in_db,
    get_games_already_in_db
)
from database.database.db_interface import DBInterface
from database.database.models import Player, Game, Month, Move
import time
from datetime import datetime
from database.operations import players as players_ops # Explicitly import players_ops
# from database.operations.chess_com_api import get_profile # Uncomment if get_profile is used here directly, though it's used via players_ops.insert_player


# Define a global ThreadPoolExecutor for CPU-bound tasks.
# It's better to manage this at the application level (e.g., FastAPI's lifespan events)
# for a long-running service. For this example, we'll initialize it here.
# Max workers is set to the number of CPU cores to maximize CPU-bound parallelism.
cpu_bound_executor = concurrent.futures.ThreadPoolExecutor(max_workers=mp.cpu_count())


# --- ASYNC FUNCTIONS (for database interactions or orchestration) ---

# Note: The insert_player function here is from the previous context,
# but it's generally better to rely on players_ops.insert_player directly.
# Keeping it here for reference to what it might do if called directly.
# This function is not used in the current format_games, but if it were uncommented
# and used, it would need get_profile imported.
# async def insert_player(data: dict):
#     player_interface = DBInterface(Player)
#     player_name = data['player_name'].lower()

#     existing_player_data = await player_interface.read_by_name(player_name)
#     if existing_player_data:
#         # Assuming existing_player_data is a dict from read_by_name
#         return PlayerCreateData(**existing_player_data)

#     profile = await get_profile(player_name) # get_profile returns PlayerCreateData or None

#     if profile is None:
#         print(f"Failed to fetch or process profile for {player_name}. Cannot insert.")
#         return None

#     player_data_to_insert = profile.model_dump() # Convert Pydantic model to dict

#     try:
#         created_player_orm = await player_interface.create(player_data_to_insert)
#         return PlayerCreateData(**player_interface.to_dict(created_player_orm))
#     except Exception as e:
#         print(f"Error inserting player {player_name} into database: {e}")
#         return None


async def insert_new_data(games_list, moves_list, months_list):
    """
    Inserts formatted game, move, and month data into the database concurrently.
    """
    game_interface = DBInterface(Game)
    move_interface = DBInterface(Move)
    month_interface = DBInterface(Month)

    # Use asyncio.gather to run bulk insertions concurrently for efficiency
    await asyncio.gather(
        game_interface.create_all(games_list),
        move_interface.create_all(moves_list),
        month_interface.create_all(months_list)
    )
    print(f"Successfully inserted {len(games_list)} games, {len(moves_list)} moves, and {len(months_list)} months.")


# --- SYNCHRONOUS HELPER FUNCTIONS (for CPU-bound data formatting) ---
# These functions do not perform I/O (no network requests, no database calls)
# so they can be run in a ThreadPoolExecutor.

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
    # This function is correct as provided.
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
    # This function is correct as provided.
    if str_result in WINING_RESULT:
        return 1.0
    if str_result in DRAW_RESULTS:
        return 0.5 # Typically 0.5 for a draw, not 0.0
    if str_result in LOSE_RESULTS:
        return 0.0 # Typically 0.0 for a loss, not -1.0
    else:
        print('""""""UNKNOWN Natural Language Result"""""""""""""')
        print(str_result)
        return None # Return None for unrecognised results, so it can be handled or flagged

def get_black_and_white_data(game, game_for_db):
    """Extracts white and black player data and results."""
    # This function is correct as provided.
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
    # This function is correct as provided.
    time_control = game['time_control']
    if "+" in time_control:
        return int(time_control.split("+")[-1])
    return 0

def get_n_moves(raw_moves):
    """Calculates the number of moves from a raw PGN moves string."""
    # This function is correct as provided.
    # Handle cases where there are no numeric move indicators or empty raw_moves
    if not raw_moves.strip():
        return 0
    numeric_moves = [int(x.replace(".", "")) for x in raw_moves.split() if x.replace(".", "").isnumeric()]
    return max(numeric_moves) if numeric_moves else 0

def create_moves_table(
        game_url:str,
        times: list,
        clean_moves: list,
        n_moves: int,
        time_bonus: int) -> dict[str, Any]: # Added Any for value type
    """Formats raw move data into a dictionary suitable for MoveCreateData."""
    # This function is correct as provided, but ensures lists are returned.
    # Ensure times and clean_moves have consistent lengths for reshaping, pad if necessary.
    # This logic assumes pairs exist for white and black moves/times.
    # If a game ends with a white move, black_moves/times might be shorter.
    # You may need more robust padding or separate processing for last move.

    # Example: If clean_moves or times is odd, add a placeholder for the last move
    if len(clean_moves) % 2 != 0:
        clean_moves.append("-")
    if len(times) % 2 != 0:
        times.append("-")

    ordered_times = np.array(times).reshape((-1, 2))
    ordered_moves = np.array(clean_moves).reshape((-1, 2))

    white_times = pd.Series(
        [pd.Timedelta(str_time).total_seconds() for str_time in ordered_times[:, 0]]
    )
    # Filter out '-' before converting to timedelta for black_times
    black_times_raw = [str_time for str_time in ordered_times[:, 1] if str_time != "-"]
    black_times = pd.Series(
        [pd.Timedelta(str_time).total_seconds() for str_time in black_times_raw]
    )

    # Use .diff() for reaction times and fillna(0) for first move
    white_reaction_times_raw = white_times.diff(periods=-1).fillna(0).abs() + time_bonus
    black_reaction_times_raw = black_times.diff(periods=-1).fillna(0).abs() + time_bonus


    result = {
        "link": int(game_url.split('/')[-1]),
        "white_moves": ordered_moves[:, 0].tolist(),
        "white_reaction_times": white_reaction_times_raw.tolist(),
        "white_time_left": white_times.tolist(),
        "black_moves": ordered_moves[:, 1].tolist(),
        "black_reaction_times": black_reaction_times_raw.tolist(),
        "black_time_left": black_times.tolist()
    }
    return result

def get_moves_data(game: dict) -> tuple[int, dict]: # Type hint for return
    """Extracts and formats move-specific data for a game."""
    time_bonus = get_time_bonus(game)

    raw_moves = (
        game['pgn'].split("\n\n")[1]
        .replace("1/2-1/2", "")
        .replace("1-0", "")
        .replace("0-1", "")
    )
    n_moves = get_n_moves(raw_moves)

    # Extract times and clean moves more robustly
    times = [x.replace("]", "").replace("}", "") for x in raw_moves.split() if ":" in x]
    # Remove text inside curly braces and then split by space, filter out empty strings and dots
    just_moves = re.sub(r"{[^}]*}*", "", raw_moves)
    clean_moves = [x for x in just_moves.split() if x and "." not in x] # Check for empty strings after split

    # Chess.com PGNs often include an extra "..." for the last move if it's black's turn to move,
    # or if the game ends on a white move without a corresponding black move.
    # The previous logic "if not f"{n_moves}..." in raw_moves:" seems slightly off.
    # A simpler approach is to ensure equal length for white/black moves/times.
    # If the number of actual moves is N, white has ceil(N/2) moves and black has floor(N/2).
    # If clean_moves is odd length, means it ended on white's move, so black_move will be empty.
    if len(clean_moves) % 2 != 0:
        clean_moves.append("--") # Placeholder for black's missing move

    if len(times) % 2 != 0:
        times.append("--") # Placeholder for black's missing time

    moves_data = create_moves_table(game['url'],
                                    times,
                                    clean_moves,
                                    n_moves,
                                    time_bonus)
    return n_moves, moves_data

def create_game_dict(game_raw_data: dict) -> Union[Dict[str, Any], str, bool]: # Type hint for return
    """Converts raw game data into a dictionary for the Game model."""
    try:
        # Check if 'pgn' exists and is not empty before proceeding
        if 'pgn' not in game_raw_data or not game_raw_data['pgn'].strip():
            print(f"Skipping game {game_raw_data.get('url', 'N/A')} due to missing or empty PGN.")
            return "NO PGN"
    except KeyError:
        print(f"Skipping game {game_raw_data.get('url', 'N/A')} due to missing PGN key.")
        return "NO PGN"

    game_for_db = dict()
    game_for_db['link'] = int(game_raw_data['url'].split('/')[-1])
    game_for_db['time_control'] = game_raw_data['time_control']
    game_for_db = get_start_and_end_date(game_raw_data, game_for_db)

    # Basic validation for essential fields after get_start_and_end_date
    if game_for_db['year'] == 0: # Indicates an issue in date parsing
        print(f"Skipping game {game_raw_data.get('url', 'N/A')} due to date parsing error.")
        return False

    game_for_db = get_black_and_white_data(game_raw_data, game_for_db)

    # Basic validation for essential player data
    if game_for_db['white_result'] is None or game_for_db['black_result'] is None:
        print(f"Skipping game {game_raw_data.get('url', 'N/A')} due to unrecognised result string.")
        return False

    try:
        n_moves, moves_data = get_moves_data(game_raw_data)
    except Exception as e:
        print(f"Error getting moves data for game {game_raw_data.get('url', 'N/A')}: {e}")
        return False

    game_for_db['n_moves'] = n_moves
    game_for_db['moves_data'] = moves_data # This will be popped later
    try:
        game_for_db['eco'] = game_raw_data['eco']
    except KeyError:
        game_for_db['eco'] = 'no_eco'
    return game_for_db

def format_one_game_moves(moves: dict) -> List[Dict[str, Any]]: # Type hint
    """Formats individual moves data for the Move model."""
    to_insert_moves = []
    try:
        # Ensure 'white_moves', 'black_moves', etc. are present and are lists
        if not all(k in moves and isinstance(moves[k], list) for k in ['white_moves', 'white_reaction_times', 'white_time_left', 'black_moves', 'black_reaction_times', 'black_time_left']):
            print(f"Warning: Missing or invalid moves data structure for game link {moves.get('link', 'N/A')}")
            return [] # Return empty list if data structure is bad
    except KeyError: # Catch if 'moves' itself is not a valid dict
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
    start_overall = time.time()
    print(f"Starting format_and_insert_games for {player_name}...")

    # Step 1: Filter out games already in DB (I/O-bound)
    start_filter = time.time()
    games_to_process = await get_just_new_games(games)
    if not games_to_process: # get_just_new_games returns False if no new games or error
        print(f"No new games to process for {player_name}. All games already at DB or input was empty.")
        return "All games already at DB"
    print(f"Filtered {sum(len(m_games) for y_games in games_to_process.values() for m_games in y_games.values())} new games in: {time.time() - start_filter:.2f} seconds")

    # Step 2: Collect all unique players from the new games and ensure they are in the Player table
    start_player_prep = time.time()
    unique_player_names = set()
    for year_games in games_to_process.values():
        for month_games in year_games.values():
            for game_raw_data in month_games:
                if 'white' in game_raw_data and 'username' in game_raw_data['white']:
                    unique_player_names.add(game_raw_data['white']['username'].lower())
                if 'black' in game_raw_data and 'username' in game_raw_data['black']:
                    unique_player_names.add(game_raw_data['black']['username'].lower())

    if not unique_player_names:
        print("No unique player names found in games to process. Skipping player insertion.")
    else:
        # Concurrently ensure all unique players exist in the Player table with full profiles
        # This calls players_ops.insert_player for each, which handles fetching/inserting.
        player_insertion_tasks = [{"player_name": p_name} for p_name in unique_player_names]
        players_ops.create_all(player_insertion_task)
        
        print(f"Ensured {len(unique_player_names)} unique players exist in DB in: {time.time() - start_player_prep:.2f} seconds")


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

    return formatted_games_results

async def insert_games_months_moves_and_players(formatted_games_results, player_name): # Added player_name as arg
    games_list_for_db = []
    moves_list_for_db = []
    # Collect month data based on successfully formatted games for this player
    # This ensures that 'n_games' accurately reflects only the games that were
    # successfully formatted and would be inserted.
    months_processed_count = {} # { (year, month): count }

    for game_dict_result in formatted_games_results:
        if game_dict_result and game_dict_result != "NO PGN" and game_dict_result != False: # Added check for 'False'
            # Safely pop moves_data to prepare for GameCreateData
            moves_data = game_dict_result.pop('moves_data', None) # Use .pop with default
            
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


    print(f"Formatted {len(games_list_for_db)} games and {len(moves_list_for_db)} moves in: {time.time() - start_format:.2f} seconds") # This print refers to 'start_format' from the calling func. Could be misleading.

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
    # No need for all_games_flat if we reconstruct the dict directly later

    # Flatten the nested dictionary and collect all links
    for year_data in games.values():
        for month_data in year_data.values():
            for game in month_data:
                try:
                    # Ensure 'pgn' and 'url' exist before processing
                    if 'pgn' in game and game['pgn'] and 'url' in game and game['url']:
                        game_link_str = game['url'].split('/')[-1]
                        if game_link_str.isdigit(): # Ensure it's a valid integer link
                            links_to_check.add(int(game_link_str))
                        else:
                            print(f"Warning: Skipping game with invalid URL link format: {game.get('url')}")
                    else:
                        print(f"Warning: Skipping game due to missing or empty PGN/URL: {game.get('url', 'N/A')}")
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
        print("After filtering, no new games remain. This might indicate an issue with link matching.")
        return False

    return new_games_structured





# # OPERATIONS FORMAT GAMES (REVISED for Asynchronous Execution and Efficiency)
# from typing import Union,Dict,Any, List
# import asyncio
# import concurrent.futures
# from fastapi.encoders import jsonable_encoder
# import numpy as np
# from constants import DRAW_RESULTS, LOSE_RESULTS, WINING_RESULT # Assuming these are defined correctly
# from .models import PlayerCreateData, GameCreateData, MoveCreateData, MonthCreateData
# import re
# import pandas as pd
# import multiprocessing as mp # Used for setting ThreadPoolExecutor max_workers
# from database.database.ask_db import (
#     get_players_already_in_db,
#     get_games_already_in_db
# )
# from database.database.db_interface import DBInterface
# from database.database.models import Player, Game, Month, Move
# import time
# from datetime import datetime
# from database.operations import players as players_ops # Explicitly import players_ops

# # Define a global ThreadPoolExecutor for CPU-bound tasks.
# # It's better to manage this at the application level (e.g., FastAPI's lifespan events)
# # for a long-running service. For this example, we'll initialize it here.
# # Max workers is set to the number of CPU cores to maximize CPU-bound parallelism.
# cpu_bound_executor = concurrent.futures.ThreadPoolExecutor(max_workers=mp.cpu_count())


# # --- ASYNC FUNCTIONS (for database interactions or orchestration) ---

# # # Note: The insert_player function here is from the previous context,
# # # but it's generally better to rely on players_ops.insert_player directly.
# # # Keeping it here for reference to what it might do if called directly.
# # async def insert_player(data: dict):
# #     player_interface = DBInterface(Player)
# #     player_name = data['player_name'].lower()

# #     existing_player_data = await player_interface.read_by_name(player_name)
# #     if existing_player_data:
# #         # Assuming existing_player_data is a dict from read_by_name
# #         return PlayerCreateData(**existing_player_data)

# #     profile = await get_profile(player_name) # get_profile returns PlayerCreateData or None

# #     if profile is None:
# #         print(f"Failed to fetch or process profile for {player_name}. Cannot insert.")
# #         return None

# #     player_data_to_insert = profile.model_dump() # Convert Pydantic model to dict

# #     try:
# #         created_player_orm = await player_interface.create(player_data_to_insert)
# #         return PlayerCreateData(**player_interface.to_dict(created_player_orm))
# #     except Exception as e:
# #         print(f"Error inserting player {player_name} into database: {e}")
# #         return None


# async def insert_new_data(games_list, moves_list, months_list):
#     """
#     Inserts formatted game, move, and month data into the database concurrently.
#     """
#     game_interface = DBInterface(Game)
#     move_interface = DBInterface(Move)
#     month_interface = DBInterface(Month)

#     # Use asyncio.gather to run bulk insertions concurrently for efficiency
#     await asyncio.gather(
#         game_interface.create_all(games_list),
#         move_interface.create_all(moves_list),
#         month_interface.create_all(months_list)
#     )
#     print(f"Successfully inserted {len(games_list)} games, {len(moves_list)} moves, and {len(months_list)} months.")


# # --- SYNCHRONOUS HELPER FUNCTIONS (for CPU-bound data formatting) ---
# # These functions do not perform I/O (no network requests, no database calls)
# # so they can be run in a ThreadPoolExecutor.

# def get_pgn_item(game_pgn: str, item: str) -> str:
#     """Extracts an item from a PGN string."""
#     # This function is correct as provided.
#     if item == "Termination":
#         return (
#             game_pgn.split(f"{item}")[1]
#             .split("\n")[0]
#             .replace('"', "")
#             .replace("]", "")
#             .lower()
#         )
#     return (
#         game_pgn.split(f"{item}")[1]
#         .split("\n")[0]
#         .replace('"', "")
#         .replace("]", "")
#         .replace(" ", "")
#         .lower()
#     )

# def get_start_and_end_date(game, game_for_db):
#     """Extracts and calculates game start/end dates and time elapsed."""
#     # This function is correct as provided.
#     try:
#         game_date = get_pgn_item(game['pgn'], item='Date').split('.')
#         game_for_db['year'] = int(game_date[0])
#         game_for_db['month'] = int(game_date[1])
#         game_for_db['day'] = int(game_date[2])
#     except Exception as e:
#         print(f"Warning: Could not parse game date for game {game.get('url', 'N/A')}. Setting year to 0. Error: {e}")
#         game_for_db['year'] = 0
#         return game_for_db

#     try:
#         game_start_time_str = get_pgn_item(game['pgn'], item='StartTime').split(':')
#         game_for_db['hour'] = int(game_start_time_str[0])
#         game_for_db['minute'] = int(game_start_time_str[1])
#         game_for_db['second'] = int(game_start_time_str[2])
#     except Exception as e:
#         print(f"Warning: Could not parse game start time for game {game.get('url', 'N/A')}. Setting to 0. Error: {e}")
#         game_for_db['hour'] = 0
#         game_for_db['minute'] = 0
#         game_for_db['second'] = 0


#     game_start = datetime(year = game_for_db['year'],
#                           month = game_for_db['month'],
#                           day = game_for_db['day'],
#                           hour = game_for_db['hour'],
#                           minute = game_for_db['minute'],
#                           second = game_for_db['second'])

#     try:
#         game_end_date_str = get_pgn_item(game['pgn'], item='EndDate').split('.')
#         game_for_db['end_year'] = int(game_end_date_str[0])
#         game_for_db['end_month'] = int(game_end_date_str[1])
#         game_for_db['end_day'] = int(game_end_date_str[2])
#         game_end_time_str = get_pgn_item(game['pgn'], item='EndTime').split(':')
#         game_for_db['end_hour'] = int(game_end_time_str[0])
#         game_for_db['end_minute'] = int(game_end_time_str[1])
#         game_for_db['end_second'] = int(game_end_time_str[2])

#         game_end = datetime(year= game_for_db['end_year'],
#                             month = game_for_db['end_month'],
#                             day = game_for_db['end_day'],
#                             hour = game_for_db['end_hour'],
#                             minute = game_for_db['end_minute'],
#                             second =game_for_db['end_second'])
#         game_for_db['time_elapsed'] = (game_end - game_start).total_seconds() # Use total_seconds
#     except Exception as e:
#         print(f"Warning: Could not parse game end date/time or calculate time_elapsed for game {game.get('url', 'N/A')}. Setting to 0. Error: {e}")
#         game_for_db['end_year'] = 0
#         game_for_db['end_month'] = 0
#         game_for_db['end_day'] = 0
#         game_for_db['end_hour'] = 0
#         game_for_db['end_minute'] = 0
#         game_for_db['end_second'] = 0
#         game_for_db['time_elapsed'] = 0

#     return game_for_db

# def translate_result_to_float(str_result):
#     """Converts string results to float representation."""
#     # This function is correct as provided.
#     if str_result in WINING_RESULT:
#         return 1.0
#     if str_result in DRAW_RESULTS:
#         return 0.5 # Typically 0.5 for a draw, not 0.0
#     if str_result in LOSE_RESULTS:
#         return 0.0 # Typically 0.0 for a loss, not -1.0
#     else:
#         print('""""""UNKNOWN Natural Language Result"""""""""""""')
#         print(str_result)
#         return None # Return None for unrecognised results, so it can be handled or flagged

# def get_black_and_white_data(game, game_for_db):
#     """Extracts white and black player data and results."""
#     # This function is correct as provided.
#     game_for_db['black'] = game['black']['username'].lower()
#     game_for_db['black_elo'] = int(game['black']['rating'])
#     game_for_db['black_str_result'] = game['black']['result'].lower()
#     game_for_db['black_result'] = translate_result_to_float(game_for_db['black_str_result'])

#     game_for_db['white'] = game['white']['username'].lower()
#     game_for_db['white_elo'] = int(game['white']['rating'])
#     game_for_db['white_str_result'] = game['white']['result'].lower()
#     game_for_db['white_result'] = translate_result_to_float(game_for_db['white_str_result'])
#     return game_for_db

# def get_time_bonus(game):
#     """Extracts time bonus from time_control string."""
#     # This function is correct as provided.
#     time_control = game['time_control']
#     if "+" in time_control:
#         return int(time_control.split("+")[-1])
#     return 0

# def get_n_moves(raw_moves):
#     """Calculates the number of moves from a raw PGN moves string."""
#     # This function is correct as provided.
#     # Handle cases where there are no numeric move indicators or empty raw_moves
#     if not raw_moves.strip():
#         return 0
#     numeric_moves = [int(x.replace(".", "")) for x in raw_moves.split() if x.replace(".", "").isnumeric()]
#     return max(numeric_moves) if numeric_moves else 0

# def create_moves_table(
#         game_url:str,
#         times: list,
#         clean_moves: list,
#         n_moves: int,
#         time_bonus: int) -> dict[str]:
#     """Formats raw move data into a dictionary suitable for MoveCreateData."""
#     # This function is correct as provided, but ensures lists are returned.
#     # Ensure times and clean_moves have consistent lengths for reshaping, pad if necessary.
#     # This logic assumes pairs exist for white and black moves/times.
#     # If a game ends with a white move, black_moves/times might be shorter.
#     # You may need more robust padding or separate processing for last move.

#     # Example: If clean_moves or times is odd, add a placeholder for the last move
#     if len(clean_moves) % 2 != 0:
#         clean_moves.append("-")
#     if len(times) % 2 != 0:
#         times.append("-")

#     ordered_times = np.array(times).reshape((-1, 2))
#     ordered_moves = np.array(clean_moves).reshape((-1, 2))

#     white_times = pd.Series(
#         [pd.Timedelta(str_time).total_seconds() for str_time in ordered_times[:, 0]]
#     )
#     # Filter out '-' before converting to timedelta for black_times
#     black_times_raw = [str_time for str_time in ordered_times[:, 1] if str_time != "-"]
#     black_times = pd.Series(
#         [pd.Timedelta(str_time).total_seconds() for str_time in black_times_raw]
#     )

#     # Use .diff() for reaction times and fillna(0) for first move
#     white_reaction_times_raw = white_times.diff(periods=-1).fillna(0).abs() + time_bonus
#     black_reaction_times_raw = black_times.diff(periods=-1).fillna(0).abs() + time_bonus


#     result = {
#         "link": int(game_url.split('/')[-1]),
#         "white_moves": ordered_moves[:, 0].tolist(),
#         "white_reaction_times": white_reaction_times_raw.tolist(),
#         "white_time_left": white_times.tolist(),
#         "black_moves": ordered_moves[:, 1].tolist(),
#         "black_reaction_times": black_reaction_times_raw.tolist(),
#         "black_time_left": black_times.tolist()
#     }
#     return result

# def get_moves_data(game: dict) -> tuple[int, dict]: # Type hint for return
#     """Extracts and formats move-specific data for a game."""
#     time_bonus = get_time_bonus(game)

#     raw_moves = (
#         game['pgn'].split("\n\n")[1]
#         .replace("1/2-1/2", "")
#         .replace("1-0", "")
#         .replace("0-1", "")
#     )
#     n_moves = get_n_moves(raw_moves)

#     # Extract times and clean moves more robustly
#     times = [x.replace("]", "").replace("}", "") for x in raw_moves.split() if ":" in x]
#     # Remove text inside curly braces and then split by space, filter out empty strings and dots
#     just_moves = re.sub(r"{[^}]*}*", "", raw_moves)
#     clean_moves = [x for x in just_moves.split() if x and "." not in x] # Check for empty strings after split

#     # Chess.com PGNs often include an extra "..." for the last move if it's black's turn to move,
#     # or if the game ends on a white move without a corresponding black move.
#     # The previous logic "if not f"{n_moves}..." in raw_moves:" seems slightly off.
#     # A simpler approach is to ensure equal length for white/black moves/times.
#     # If the number of actual moves is N, white has ceil(N/2) moves and black has floor(N/2).
#     # If clean_moves is odd length, means it ended on white's move, so black_move will be empty.
#     if len(clean_moves) % 2 != 0:
#         clean_moves.append("--") # Placeholder for black's missing move

#     if len(times) % 2 != 0:
#         times.append("--") # Placeholder for black's missing time

#     moves_data = create_moves_table(game['url'],
#                                     times,
#                                     clean_moves,
#                                     n_moves,
#                                     time_bonus)
#     return n_moves, moves_data

# def create_game_dict(game_raw_data: dict) -> Union[Dict[str, Any], str, bool]: # Type hint for return
#     """Converts raw game data into a dictionary for the Game model."""
#     try:
#         # Check if 'pgn' exists and is not empty before proceeding
#         if 'pgn' not in game_raw_data or not game_raw_data['pgn'].strip():
#             print(f"Skipping game {game_raw_data.get('url', 'N/A')} due to missing or empty PGN.")
#             return "NO PGN"
#     except KeyError:
#         print(f"Skipping game {game_raw_data.get('url', 'N/A')} due to missing PGN key.")
#         return "NO PGN"

#     game_for_db = dict()
#     game_for_db['link'] = int(game_raw_data['url'].split('/')[-1])
#     game_for_db['time_control'] = game_raw_data['time_control']
#     game_for_db = get_start_and_end_date(game_raw_data, game_for_db)

#     # Basic validation for essential fields after get_start_and_end_date
#     if game_for_db['year'] == 0: # Indicates an issue in date parsing
#         print(f"Skipping game {game_raw_data.get('url', 'N/A')} due to date parsing error.")
#         return False

#     game_for_db = get_black_and_white_data(game_raw_data, game_for_db)

#     # Basic validation for essential player data
#     if game_for_db['white_result'] is None or game_for_db['black_result'] is None:
#         print(f"Skipping game {game_raw_data.get('url', 'N/A')} due to unrecognised result string.")
#         return False

#     try:
#         n_moves, moves_data = get_moves_data(game_raw_data)
#     except Exception as e:
#         print(f"Error getting moves data for game {game_raw_data.get('url', 'N/A')}: {e}")
#         return False

#     game_for_db['n_moves'] = n_moves
#     game_for_db['moves_data'] = moves_data # This will be popped later
#     try:
#         game_for_db['eco'] = game_raw_data['eco']
#     except KeyError:
#         game_for_db['eco'] = 'no_eco'
#     return game_for_db

# def format_one_game_moves(moves: dict) -> List[Dict[str, Any]]: # Type hint
#     """Formats individual moves data for the Move model."""
#     to_insert_moves = []
#     try:
#         # Ensure 'white_moves', 'black_moves', etc. are present and are lists
#         if not all(k in moves and isinstance(moves[k], list) for k in ['white_moves', 'white_reaction_times', 'white_time_left', 'black_moves', 'black_reaction_times', 'black_time_left']):
#             print(f"Warning: Missing or invalid moves data structure for game link {moves.get('link', 'N/A')}")
#             return [] # Return empty list if data structure is bad
#     except KeyError: # Catch if 'moves' itself is not a valid dict
#         return []

#     # Ensure all lists are of comparable length, or handle index errors gracefully
#     max_len = len(moves['white_moves'])

#     for ind in range(max_len):
#         moves_dict = {}
#         moves_dict['n_move'] = ind + 1
#         moves_dict['link'] = moves['link']

#         # White's move data
#         moves_dict['white_move'] = str(moves['white_moves'][ind])
#         moves_dict['white_reaction_time'] = round(moves['white_reaction_times'][ind], 3) if ind < len(moves['white_reaction_times']) else 0.0
#         moves_dict['white_time_left'] = round(moves['white_time_left'][ind], 3) if ind < len(moves['white_time_left']) else 0.0

#         # Black's move data (handle potential IndexError if black has fewer moves)
#         try:
#             moves_dict['black_move'] = str(moves['black_moves'][ind])
#             moves_dict['black_reaction_time'] = round(moves['black_reaction_times'][ind], 3) if ind < len(moves['black_reaction_times']) else 0.0
#             moves_dict['black_time_left'] = round(moves['black_time_left'][ind], 3) if ind < len(moves['black_time_left']) else 0.0
#         except IndexError:
#             moves_dict['black_move'] = '--'
#             moves_dict['black_reaction_time'] = 0.0
#             moves_dict['black_time_left'] = 0.0

#         # Validate and convert to Pydantic model, then dump to dict
#         try:
#             to_insert_moves.append(MoveCreateData(**moves_dict).model_dump())
#         except Exception as e:
#             print(f"Error creating MoveCreateData for move {ind+1} of game {moves.get('link', 'N/A')}: {e}")
#             # Decide whether to skip this move or the whole game, for now just skip this move
#             continue
#     return to_insert_moves


# # --- MAIN FORMAT AND INSERT FUNCTION ---

# async def format_games(games, player_name):
#     """
#     Formats and inserts downloaded games into the database efficiently.

#     Args:
#         games (dict): Dictionary of downloaded games, structured by year and month.
#                       E.g., { '2023': { '01': [game_obj1, game_obj2], ... }, ... }
#         player_name (str): The name of the player for whom games are being processed.

#     Returns:
#         str: A message indicating the outcome of the operation.
#     """
#     start_overall = time.time()
#     print(f"Starting format_and_insert_games for {player_name}...")

#     # Step 1: Filter out games already in DB (I/O-bound)
#     start_filter = time.time()
#     games_to_process = await get_just_new_games(games)
#     if not games_to_process: # get_just_new_games returns False if no new games or error
#         print(f"No new games to process for {player_name}. All games already at DB or input was empty.")
#         return "All games already at DB"
#     print(f"Filtered {sum(len(m_games) for y_games in games_to_process.values() for m_games in y_games.values())} new games in: {time.time() - start_filter:.2f} seconds")

#     # Step 2: Collect all unique players from the new games and ensure they are in the Player table
#     start_player_prep = time.time()
#     unique_player_names = set()
#     for year_games in games_to_process.values():
#         for month_games in year_games.values():
#             for game_raw_data in month_games:
#                 if 'white' in game_raw_data and 'username' in game_raw_data['white']:
#                     unique_player_names.add(game_raw_data['white']['username'].lower())
#                 if 'black' in game_raw_data and 'username' in game_raw_data['black']:
#                     unique_player_names.add(game_raw_data['black']['username'].lower())

#     if not unique_player_names:
#         print("No unique player names found in games to process. Skipping player insertion.")
#     else:
#         # Concurrently ensure all unique players exist in the Player table with full profiles
#         # This calls players_ops.insert_player for each, which handles fetching/inserting.
#         player_insertion_tasks = [players_ops.insert_player({"player_name": p_name}) for p_name in unique_player_names]
#         await asyncio.gather(*player_insertion_tasks)
#         print(f"Ensured {len(unique_player_names)} unique players exist in DB in: {time.time() - start_player_prep:.2f} seconds")


#     # Step 3: Format games and moves (CPU-bound, offload to executor)
#     start_format = time.time()
#     games_futures = []
#     for year in games_to_process.keys():
#         for month in games_to_process[year].keys():
#             # Create a list of futures for CPU-bound game formatting
#             for game_raw_data in games_to_process[year][month]:
#                 # Offload create_game_dict to a separate thread
#                 game_future = asyncio.to_thread(create_game_dict, game_raw_data)
#                 games_futures.append(game_future)

#     # Await all game formatting futures concurrently
#     formatted_games_results = await asyncio.gather(*games_futures)

#     return formatted_games_results

# async def insert_games_months_moves_and_players(formatted_games_results):
#     games_list_for_db = []
#     moves_list_for_db = []
#     # Collect month data based on successfully formatted games for this player
#     # This ensures that 'n_games' accurately reflects only the games that were
#     # successfully formatted and would be inserted.
#     months_processed_count = {} # { (year, month): count }

#     for game_dict_result in formatted_games_results:
#         if game_dict_result and game_dict_result != "NO PGN":
#             # Safely pop moves_data to prepare for GameCreateData
#             moves_data = game_dict_result.pop('moves_data', None) # Use .pop with default
            
#             if moves_data:
#                 moves_formatted_future = asyncio.to_thread(format_one_game_moves, moves_data)
#                 moves_list_for_db.extend(await moves_formatted_future)

#             # Ensure the game_dict_result can be parsed by GameCreateData
#             try:
#                 games_list_for_db.append(GameCreateData(**game_dict_result).model_dump())
                
#                 # Update month counts for the current player
#                 game_year = game_dict_result.get('year')
#                 game_month = game_dict_result.get('month')
#                 if game_year and game_month:
#                     key = (int(game_year), int(game_month))
#                     months_processed_count[key] = months_processed_count.get(key, 0) + 1

#             except Exception as e:
#                 print(f"Error creating GameCreateData for formatted game {game_dict_result.get('link', 'N/A')}: {e}. Skipping game.")
#                 continue

#     # Create months_list_for_db from the collected counts
#     months_list_for_db = []
#     for (year, month), n_games in months_processed_count.items():
#         month_data = {
#             "player_name": player_name,
#             "year": year,
#             "month": month,
#             "n_games": n_games
#         }
#         months_list_for_db.append(MonthCreateData(**month_data).model_dump())


#     print(f"Formatted {len(games_list_for_db)} games and {len(moves_list_for_db)} moves in: {time.time() - start_format:.2f} seconds")

#     # Step 4: Insert data into DB (I/O-bound, run concurrently)
#     if not games_list_for_db and not moves_list_for_db and not months_list_for_db:
#         print("No data to insert after formatting. Skipping database insertion.")
#         return f"No new data to insert for {player_name}."

#     start_insert = time.time()
#     await insert_new_data(games_list_for_db, moves_list_for_db, months_list_for_db)
#     print(f'Inserted games, moves, and months for {len(games_list_for_db)} games in: {time.time()-start_insert:.2f} seconds')

#     end_overall = time.time()
#     print(f"Total time for format_and_insert_games for {player_name}: {(end_overall-start_overall):.2f} seconds")

#     return f"Successfully processed and inserted {len(games_list_for_db)} games for {player_name}."

# async def get_just_new_games(games):
    
#     links = []
#     new_games = {}
#     for year in games.keys():
#         for month in games[year].keys():
#             for game in games[year][month]:
#                 try:
#                     game['pgn']
#                     links.append(game['url'].split('/')[-1])
#                 except:
#                     continue
#     #print(links)
#     in_db_games = get_games_already_in_db(tuple(links))
#     to_insert_games = set([int(x) for x in links]) - in_db_games
   
#     if len(to_insert_games)==0:
#         return False
    
#     count = 0
#     for year in games.keys():
#         new_games[year] = {}
#         for month in games[year].keys():
#             new_games[year][month] = []
#             for game in games[year][month]:
#                 if int(game['url'].split('/')[-1]) in to_insert_games:
#                         new_games[year][month].append(game)
#                         count += 1
#     if count ==0:
#         return False
#     return new_games


# # OPERATIONS FORMAT GAMES (REVISED for Asynchronous Execution)

# from fastapi.encoders import jsonable_encoder

# import numpy as np
# from constants import DRAW_RESULTS, LOSE_RESULTS, WINING_RESULT, CONN_STRING
# from .models import PlayerCreateData, GameCreateData, MoveCreateData, MonthCreateData
# import re
# import pandas as pd
# import multiprocessing as mp # Keep for ThreadPoolExecutor for CPU-bound tasks
# import concurrent # Keep for ThreadPoolExecutor for CPU-bound tasks
# from database.database.ask_db import (
#     get_players_already_in_db,
#     get_games_already_in_db
# ) # No need for player_exists_at_db or open_request if handled by DBInterface
# from database.database.db_interface import DBInterface
# from database.database.models import Player, Game, Month, Move
# import time
# from datetime import datetime
# from database.operations.chess_com_api import get_profile

# # Define a global ThreadPoolExecutor for CPU-bound tasks, if needed.
# # This should be managed carefully, e.g., using FastAPI's lifespan events.
# # For simplicity, we'll create it here for now.
# # cpu_executor = concurrent.futures.ThreadPoolExecutor(max_workers=mp.cpu_count())


# # --- ASYNC FUNCTIONS ---
# async def insert_player(data: dict):
#     player_interface = DBInterface(Player) # Assuming DBInterface is now async-ready
#     player_name = data['player_name'].lower()

#     # Check if player exists asynchronously
#     if await player_interface.player_exists(player_name):
#         return await player_interface.read_by_name(player_name)

#     profile = await get_profile(player_name) # get_profile is already async
    
#     if profile is None: # get_profile returns None on error or not found
#         print(f"Failed to fetch or process profile for {player_name}. Cannot insert.")
#         return None # Return None or raise an appropriate exception

#     # Ensure profile is a dict before trying to access keys, or handle Pydantic model directly
#     if isinstance(profile, PlayerCreateData):
#         profile_data = profile.model_dump()
#     else: # This path should ideally not be taken if get_profile returns PlayerCreateData or None
#         profile_data = profile # If get_profile ever returns a raw dict, handle it

#     profile_data['player_name'] = player_name # Ensure consistency
#     player_data = PlayerCreateData(**profile_data) # Re-validate if coming from raw dict

#     await player_interface.create(player_data.model_dump())
#     return jsonable_encoder(player_data)


# # These functions are CPU-bound, they can remain synchronous or be wrapped in run_in_executor
# # if they become a bottleneck and you want to use the event loop for other things.
# # For now, let's keep them sync and consider `run_in_executor` for `format_one_game_moves`
# # or the entire game processing loop within format_and_insert_games.

# def get_pgn_item(game_pgn: str, item: str) -> str:
#     # ... (no change, this is sync) ...
#     if item == "Termination":
#         return (
#             game_pgn.split(f"{item}")[1]
#             .split("\n")[0]
#             .replace('"', "")
#             .replace("]", "")
#             .lower()
#         )
#     return (
#         game_pgn.split(f"{item}")[1]
#         .split("\n")[0]
#         .replace('"', "")
#         .replace("]", "")
#         .replace(" ", "")
#         .lower()
#     )

# def get_start_and_end_date(game, game_for_db):
#     # ... (no change, this is sync) ...
#     try:
#         game_date = get_pgn_item(game['pgn'], item='Date').split('.')
#     except:
#         game_for_db['year'] = 0
#         return game_for_db
        
#     game_for_db['year'] = int(game_date[0])
#     game_for_db['month'] = int(game_date[1])
#     game_for_db['day'] = int(game_date[2])
    
#     game_start = get_pgn_item(game['pgn'], item='StartTime').split(':')
#     game_for_db['hour'] =  int(game_start[0])
#     game_for_db['minute'] = int(game_start[1])
#     game_for_db['second'] = int(game_start[2])

#     game_start = datetime(year = game_for_db['year'],
#                           month = game_for_db['month'],
#                           day = game_for_db['day'],
#                           hour = game_for_db['hour'],
#                           minute = game_for_db['minute'],
#                           second = game_for_db['second'])
    
#     game_end_date = get_pgn_item(game['pgn'], item='EndDate').split('.')
#     game_for_db['end_year'] = int(game_end_date[0])
#     game_for_db['end_month'] = int(game_end_date[1])
#     game_for_db['end_day'] = int(game_end_date[2])
#     game_end_time = get_pgn_item(game['pgn'], item='EndTime').split(':')
#     game_for_db['end_hour'] = int(game_end_time[0])
#     game_for_db['end_minute'] = int(game_end_time[1])
#     game_for_db['end_second'] = int(game_end_time[2])

#     game_end = datetime(
#                         year= game_for_db['end_year'],
#                         month = game_for_db['end_month'],
#                         day = game_for_db['end_day'],
#                         hour = game_for_db['end_hour'],
#                         minute = game_for_db['end_minute'],
#                         second =game_for_db['end_second']
#     )

#     game_for_db['time_elapsed'] = (game_end - game_start).seconds
#     return game_for_db

# def translate_result_to_float(str_result):
#     # ... (no change, this is sync) ...
#     if str_result in WINING_RESULT:
#         return 1.0
#     if str_result in DRAW_RESULTS:
#         return 0.0
#     if str_result in LOSE_RESULTS:
#         return -1.0
#     else:
#         print('""""""UNKNOWN Natural Lenguague Result"""""""""""""')
#         print(str_result)
#         return str_result

# def get_black_and_white_data(game, game_for_db):
#     # ... (no change, this is sync) ...
#     game_for_db['black'] = game['black']['username'].lower()
#     game_for_db['black_elo'] = int(game['black']['rating'])
#     game_for_db['black_str_result'] = game['black']['result'].lower()
#     game_for_db['black_result'] = translate_result_to_float(game_for_db['black_str_result'])

#     game_for_db['white'] = game['white']['username'].lower()
#     game_for_db['white_elo'] = int(game['white']['rating'])
#     game_for_db['white_str_result'] = game['white']['result'].lower()
#     game_for_db['white_result'] = translate_result_to_float(game_for_db['white_str_result'])
#     return game_for_db

# def get_time_bonus(game):
#     # ... (no change, this is sync) ...
#     time_control = game['time_control']
#     if "+" in time_control:
#         return int(time_control.split("+")[-1])
#     return 0

# def get_n_moves(raw_moves):
#     # ... (no change, this is sync) ...
#     return max(
#         [
#             int(x.replace(".", ""))
#             for x in raw_moves.split()
#             if x.replace(".", "").isnumeric()
#         ]
#     )

# def create_moves_table(
#         game_url:str,
#         times: list,
#         clean_moves: list,
#         n_moves: int,
#         time_bonus: int) -> dict[str]:
#     # ... (no change, this is sync) ...
#     ordered_times = np.array(times).reshape((-1, 2))
#     ordered_moves = np.array(clean_moves).reshape((-1, 2))
#     white_times = pd.Series(
#         [
#             pd.Timedelta(str(str_time)).total_seconds()
#             for str_time in ordered_times[:, 0]
#         ]
#     )
#     black_times = pd.Series(
#         [
#             pd.Timedelta(str(str_time)).total_seconds()
#             for str_time in ordered_times[:, 1]
#             if str_time != "-"
#         ]
#     )
#     white_cumsub = white_times.sub(white_times.shift(-1)) + time_bonus
#     black_cumsub = black_times.sub(black_times.shift(-1)) + time_bonus

#     result = {
#         "link": int(game_url.split('/')[-1]),
#         "white_moves": ordered_moves[:, 0].tolist(), # Convert numpy array to list for Pydantic
#         "white_reaction_times": white_cumsub.tolist(),
#         "white_time_left": white_times.tolist(),
#         "black_moves": ordered_moves[:, 1].tolist(),
#         "black_reaction_times": black_cumsub.tolist(),
#         "black_time_left": black_times.tolist()
#     }
#     return result

# def get_moves_data(game: dict) -> tuple: # Changed game type hint to dict
#     time_bonus = get_time_bonus(game)
        
#     raw_moves = (
#         game['pgn'].split("\n\n")[1]
#         .replace("1/2-1/2", "")
#         .replace("1-0", "")
#         .replace("0-1", "")
#     )
#     n_moves = get_n_moves(raw_moves)
#     times = [x.replace("]", "").replace("}", "") for x in raw_moves.split() if ":" in x]
#     just_moves = re.sub(r"{[^}]*}*", "", raw_moves)
#     clean_moves = [x for x in just_moves.split() if "." not in x]
#     if not f"{n_moves}..." in raw_moves:
#         clean_moves.append("-")
#         times.append("-")
#     moves_data = create_moves_table(game['url'],
#                                     times,
#                                     clean_moves,
#                                     n_moves,
#                                     time_bonus,
#                                     )
#     return n_moves, moves_data

# def create_game_dict(game_raw_data: dict): # Renamed arg for clarity
#     try:
#         game_raw_data['pgn']
#     except KeyError: # More specific exception
#         return "NO PGN"
            
#     game_for_db = dict()
#     game_for_db['link'] = int(game_raw_data['url'].split('/')[-1])
#     game_for_db['time_control']  = game_raw_data['time_control']
#     game_for_db = get_start_and_end_date(game_raw_data, game_for_db)
#     game_for_db = get_black_and_white_data(game_raw_data, game_for_db)
#     try:
#         n_moves, moves_data = get_moves_data(game_raw_data) # Pass raw game dict
#     except Exception as e: # Catch specific errors or log more
#         print(f"Error getting moves data for game {game_raw_data.get('url', 'N/A')}: {e}")
#         return False
            
#     game_for_db['n_moves']  = n_moves
#     game_for_db['moves_data']  = moves_data
#     try:
#         game_for_db['eco']  = game_raw_data['eco']
#     except KeyError: # More specific
#         game_for_db['eco']  = 'no_eco'
#     return game_for_db

# def format_one_game_moves(moves: dict): # Type hint
#     to_insert_moves = []
#     try:
#         moves['white_moves']
#     except KeyError:
#         return False
#     for ind, white_move in enumerate(moves['white_moves']):
#         moves_dict = {}
#         moves_dict['n_move'] = ind + 1
#         moves_dict['link'] = moves['link']
        
#         moves_dict['white_move'] = str(white_move) # Ensure string conversion
#         moves_dict['white_reaction_time'] = round(moves['white_reaction_times'][ind],3)
#         moves_dict['white_time_left'] = round(moves['white_time_left'][ind],3)

#         try:
#             moves_dict['black_move'] = str(moves['black_moves'][ind]) # Ensure string conversion
#             moves_dict['black_reaction_time'] = round(moves['black_reaction_times'][ind],3)
#             moves_dict['black_time_left'] = round(moves['black_time_left'][ind],3)
#         except IndexError: # If black move doesn't exist for this index (e.g., last move)
#             moves_dict['black_move'] = '--'
#             moves_dict['black_reaction_time'] = 0.0
#             moves_dict['black_time_left'] = 0.0
        
#         to_insert_moves.append(MoveCreateData(**moves_dict).model_dump())
#     return to_insert_moves

# # --- ASYNC DATABASE INTERACTION FUNCTIONS ---

# async def game_insert_players(games_list):
#     player_interface = DBInterface(Player)
#     whites = [x['white'] for x in games_list]
#     blacks = [x['black'] for x in games_list]
#     players_set = set(whites)
#     players_set.update(set(blacks))
    
#     if not players_set: # Handle empty set
#         print('no new player, this is wrong, this is very wrong')
#         return

#     in_db_players = await get_players_already_in_db(tuple(players_set)) # AWAIT
#     to_insert_players = players_set - in_db_players
    
#     if to_insert_players:
#         players_data = [PlayerCreateData(**{"player_name":x}).model_dump() for x in to_insert_players]
#         await player_interface.create_all(players_data) # AWAIT

# async def get_just_new_games(games):
#     links = []
#     for year in games.keys():
#         for month in games[year].keys():
#             for game in games[year][month]:
#                 try:
#                     # Check for 'pgn' and 'url' before appending
#                     if 'pgn' in game and 'url' in game:
#                         links.append(game['url'].split('/')[-1])
#                 except Exception as e:
#                     print(f"Error extracting link from game: {e}, game data: {game}")
#                     continue
    
#     if not links:
#         return False # No links to check

#     in_db_games = await get_games_already_in_db(tuple(links)) # AWAIT
#     to_insert_game_links = set([int(x) for x in links]) - in_db_games
    
#     if not to_insert_game_links:
#         return False
        
#     new_games_structured = {}
#     count = 0
#     for year in games.keys():
#         new_games_structured[year] = {}
#         for month in games[year].keys():
#             new_games_structured[year][month] = []
#             for game in games[year][month]:
#                 if 'url' in game and int(game['url'].split('/')[-1]) in to_insert_game_links:
#                     new_games_structured[year][month].append(game)
#                     count += 1
    
#     if count == 0:
#         return False
#     return new_games_structured

# async def insert_new_data(games_list, moves_list, months_list):
#     move_interface = DBInterface(Move)
#     game_interface = DBInterface(Game)
#     month_interface = DBInterface(Month)
    
#     # Use asyncio.gather to run insertions concurrently if your DBInterface supports it
#     await asyncio.gather(
#         game_interface.create_all(games_list),
#         month_interface.create_all(months_list),
#         move_interface.create_all(moves_list)
#     )

# # --- MAIN FORMAT AND INSERT FUNCTION ---

# async def format_and_insert_games(games, player_name):
#     # Step 1: Filter out games already in DB (async)
#     start_filter = time.time()
#     games_to_process = await get_just_new_games(games)
#     print(f"Filtered new games in: {time.time() - start_filter:.2f} seconds")

#     if not games_to_process or len(games_to_process) == 0:
#         return "All games already at DB"
    
#     games_list_for_db = []
#     moves_list_for_db = []
#     months_list_for_db = []

#     # Step 2: Format games (CPU-bound)
#     start_format = time.time()
#     for year in games_to_process.keys():
#         for month in games_to_process[year].keys():
#             month_data = {
#                 "player_name": player_name,
#                 "year": year,
#                 "month": month,
#                 "n_games": len(games_to_process[year][month])
#             }
#             months_list_for_db.append(MonthCreateData(**month_data).model_dump())

#             # Consider using run_in_executor for this inner loop if it's truly a bottleneck
#             # For now, it's synchronous within this async function, but it's CPU-bound.
#             for game_raw_data in games_to_process[year][month]:
#                 game_dict = create_game_dict(game_raw_data)
#                 if game_dict == False or game_dict == "NO PGN":
#                     # print(f"Skipping problematic game: {game_raw_data.get('url', 'N/A')}")
#                     continue # Skip problematic games
                
#                 moves_data = game_dict.pop('moves_data')
#                 moves_formatted = format_one_game_moves(moves_data)
                
#                 if moves_formatted:
#                     moves_list_for_db.extend(moves_formatted)
                
#                 games_list_for_db.append(GameCreateData(**game_dict).model_dump())
#     print(f"Formatted {len(games_list_for_db)} games in: {time.time() - start_format:.2f} seconds")

#     # Step 3: Insert data into DB (I/O-bound, run concurrently)
#     start_insert = time.time()
    
#     # First, insert players (can be done concurrently if player_insert_players is async)
#     await game_insert_players(games_list_for_db) # AWAIT this
#     print(f'Inserted players from {len(games_list_for_db)} games in: {time.time()-start_insert:.2f} seconds')

#     # Then, insert games, moves, and months concurrently
#     await insert_new_data(games_list_for_db, moves_list_for_db, months_list_for_db)
#     print(f'Inserted games, moves, and months: {len(games_list_for_db)} games in: {time.time()-start_insert:.2f} seconds')
    
#     return f"DONE DOWNLOADING AND INSERTING EVERY GAME FROM: {player_name}"


# # OPERATIONS FORMAT GAMES

# from fastapi.encoders import jsonable_encoder

# import numpy as np
# from constants import DRAW_RESULTS, LOSE_RESULTS, WINING_RESULT, CONN_STRING
# from .models import PlayerCreateData, GameCreateData, MoveCreateData, MonthCreateData
# import re
# import pandas as pd
# import multiprocessing as mp
# import concurrent
# from database.database.ask_db import (player_exists_at_db,
#                                         open_request, 
#                                         get_players_already_in_db,
#                                         get_games_already_in_db)
# from database.database.db_interface import DBInterface
# from database.database.models import Player, Game, Month, Move
# import time
# from datetime import datetime
# from database.operations.chess_com_api import get_profile

# def insert_player(data: dict):
#     player_interface = DBInterface(Player)
#     if player_interface.player_exists(data['player_name'].lower()):
#          return player_interface.read_by_name(data['player_name'].lower())
#     player_name = data['player_name']
#     profile = get_profile(player_name)
#     if type(profile)==str:
#          return profile
#     profile['player_name'] = player_name
#     player_data = PlayerCreateData(**profile)
#     player_interface.create(player_data.model_dump())
#     return jsonable_encoder(player_data)

# def get_pgn_item(game, item: str) -> str:
#     if item == "Termination":
#         return (
#             game.split(f"{item}")[1]
#             .split("\n")[0]
#             .replace('"', "")
#             .replace("]", "")
#             .lower()
#         )
#     return (
#         game.split(f"{item}")[1]
#         .split("\n")[0]
#         .replace('"', "")
#         .replace("]", "")
#         .replace(" ", "")
#         .lower()
#     )
# def get_start_and_end_date(game,game_for_db):
#     try:
#         game_date = get_pgn_item(game['pgn'], item='Date').split('.')
#     except:
#         game_for_db['year'] = 0
#         return game_for_db
    
#     game_for_db['year'] = int(game_date[0])
#     game_for_db['month'] = int(game_date[1])
#     game_for_db['day'] = int(game_date[2])
    
#     game_start = get_pgn_item(game['pgn'], item='StartTime').split(':')
#     game_for_db['hour'] =  int(game_start[0])
#     game_for_db['minute'] = int(game_start[1])
#     game_for_db['second'] = int(game_start[2])

#     game_start = datetime(year = game_for_db['year'],
#                          month = game_for_db['month'],
#                          day = game_for_db['day'],
#                          hour = game_for_db['hour'],
#                          minute = game_for_db['minute'],
#                          second = game_for_db['second'])
    
#     game_end_date = get_pgn_item(game['pgn'], item='EndDate').split('.')
#     game_for_db['end_year'] = int(game_end_date[0])
#     game_for_db['end_month'] = int(game_end_date[1])
#     game_for_db['end_day'] = int(game_end_date[2])
#     game_end_time = get_pgn_item(game['pgn'], item='EndTime').split(':')
#     game_for_db['end_hour'] = int(game_end_time[0])
#     game_for_db['end_minute'] = int(game_end_time[1])
#     game_for_db['end_second'] = int(game_end_time[2])

#     game_end = datetime(
#                 year= game_for_db['end_year'],
#                 month = game_for_db['end_month'],
#                 day = game_for_db['end_day'],
#                 hour = game_for_db['end_hour'],
#                 minute = game_for_db['end_minute'],
#                 second =game_for_db['end_second']
#     )

#     game_for_db['time_elapsed'] = (game_end - game_start).seconds
#     return game_for_db
# def translate_result_to_float(str_result):
#     DRAW_RESULTS, LOSE_RESULTS
#     if str_result in WINING_RESULT:
#         return 1.0
#     if str_result in DRAW_RESULTS:
#         return 0.0
#     if str_result in LOSE_RESULTS:
#         return -1.0
#     else:
#         print('""""""UNKNOWN Natural Lenguague Result"""""""""""""')
#         print(str_result)
#         return str_result
# def get_black_and_white_data(game, game_for_db):
#     game_for_db['black'] = game['black']['username'].lower()
#     game_for_db['black_elo'] = int(game['black']['rating'])
#     game_for_db['black_str_result'] = game['black']['result'].lower()
#     game_for_db['black_result'] = translate_result_to_float(game_for_db['black_str_result'])

#     game_for_db['white'] = game['white']['username'].lower()
#     game_for_db['white_elo'] = int(game['white']['rating'])
#     game_for_db['white_str_result'] = game['white']['result'].lower()
#     game_for_db['white_result'] = translate_result_to_float(game_for_db['white_str_result'])
#     return game_for_db

# def create_game_dict(game):
#     try:
#         game['pgn']
#     except:
#         return "NO PGN"
        
#     game_for_db = dict()
#     game_for_db['link'] = int(game['url'].split('/')[-1])
#     game_for_db['time_control']  = game['time_control']
#     game_for_db = get_start_and_end_date(game, game_for_db)
#     game_for_db = get_black_and_white_data(game, game_for_db)
#     try:
#         n_moves, moves_data = get_moves_data(game)
#     except:
#         return False
        
#     game_for_db['n_moves']  = n_moves
#     game_for_db['moves_data']  = moves_data
#     try:
#         game_for_db['eco']  = game['eco']
#     except:
#         game_for_db['eco']  = 'no_eco'
#     return game_for_db


# def get_time_bonus(game):
#     time_control = game['time_control']
#     if "+" in time_control:
#         return int(time_control.split("+")[-1])
#     return 0
# def get_n_moves(raw_moves):
#     return max(
#         [
#             int(x.replace(".", ""))
#             for x in raw_moves.split()
#             if x.replace(".", "").isnumeric()
#         ]
#     )
# def create_moves_table(
#                 game_url:str,
#                 times: list,
#                 clean_moves: list,
#                 n_moves: int,
#                 time_bonus: int) -> dict[str]:

#     ordered_times = np.array(times).reshape((-1, 2))
#     ordered_moves = np.array(clean_moves).reshape((-1, 2))
#     white_times = pd.Series(
#         [
#             pd.Timedelta(str(str_time)).total_seconds()
#             for str_time in ordered_times[:, 0]
#         ]
#     )
#     black_times = pd.Series(
#         [
#             pd.Timedelta(str(str_time)).total_seconds()
#             for str_time in ordered_times[:, 1]
#             if str_time != "-"
#         ]
#     )
#     white_cumsub = white_times.sub(white_times.shift(-1)) + time_bonus
#     black_cumsub = black_times.sub(black_times.shift(-1)) + time_bonus

#     result = {
#         "link": int(game_url.split('/')[-1]),
#         "white_moves": ordered_moves[:, 0],
#         "white_reaction_times": white_cumsub,
#         "white_time_left": white_times,
#         "black_moves": ordered_moves[:, 1],
#         "black_reaction_times": black_cumsub,
#         "black_time_left": black_times
#     }
#     return result
# def get_moves_data(game: str) -> tuple:
#     time_bonus = get_time_bonus(game)
    
        
#     raw_moves = (
#         game['pgn'].split("\n\n")[1]
#         .replace("1/2-1/2", "")
#         .replace("1-0", "")
#         .replace("0-1", "")
#     )
#     n_moves = get_n_moves(raw_moves)
#     times = [x.replace("]", "").replace("}", "") for x in raw_moves.split() if ":" in x]
#     just_moves = re.sub(r"{[^}]*}*", "", raw_moves)
#     clean_moves = [x for x in just_moves.split() if "." not in x]
#     if not f"{n_moves}..." in raw_moves:
#         clean_moves.append("-")
#         times.append("-")
#     moves_data = create_moves_table(game['url'],
#                                     times,
#                                     clean_moves,
#                                     n_moves,
#                                     time_bonus,
#                                     )
#     return n_moves, moves_data
# def format_one_game_moves(moves):
#     to_insert_moves = []
#     try:
#         moves['white_moves']
#     except:
#         return False
#     for ind, white_move in enumerate(moves['white_moves']):
#         moves_dict = {}
#         moves_dict['n_move'] = ind + 1
#         moves_dict['link'] = moves['link']
        
#         moves_dict['white_move'] = white_move
#         moves_dict['white_reaction_time'] = round(moves['white_reaction_times'][ind],3)
#         moves_dict['white_time_left'] = round(moves['white_time_left'][ind],3)

#         try:
#             moves_dict['black_move'] = moves['black_moves'][ind]
#             moves_dict['black_reaction_time'] = round(moves['black_reaction_times'][ind],3)
#             moves_dict['black_time_left'] = round(moves['black_time_left'][ind],3)
#         except:
#             moves_dict['black_move'] = '--'
#             moves_dict['black_reaction_time'] = 0.0
#             moves_dict['black_time_left'] = 0.0
#         to_insert_moves.append(MoveCreateData(**moves_dict).model_dump())
#     return to_insert_moves
# def game_insert_players(games_list):
#     player_interface = DBInterface(Player)
#     whites = [x['white'] for x in games_list]
#     blacks = [x['black'] for x in games_list]
#     players_set = set(whites)
#     players_set.update(set(blacks))
#     if len(players_set) ==0:
#         print('no new player, this is wrong, this is very wrong')
#     in_db_players = get_players_already_in_db(tuple(players_set))
#     if len(in_db_players) == 0:
#         return players_set
#     to_insert_players =  players_set - in_db_players
#     players_data = [PlayerCreateData(**{"player_name":x}).model_dump() for x in to_insert_players]
#     player_interface.create_all(players_data)

# def insert_new_data(games_list, moves_list, months_list):
#     move_interface = DBInterface(Move)
#     game_interface = DBInterface(Game)
#     month_interface = DBInterface(Month)
    
#     game_interface.create_all(games_list)
#     month_interface.create_all(months_list)
#     move_interface.create_all(moves_list)
    
    
# def format_and_insert_games(games, player_name):
#     games = get_just_new_games(games)
#     if not games or len(games)==0:
#         return "all games already at DB"
    
#     games_list = []
#     moves_list = []
#     months_list = []
#     count = 0
#     for year in games.keys():
        
#         for month in games[year].keys():
#             start = time.time()
#             month_data = {"player_name":player_name,
#                          "year":year,
#                          "month":month,
#                          "n_games":len(games[year][month])}
#             months_list.append(MonthCreateData(**month_data).model_dump())
#             for game in games[year][month]:
#                 game = create_game_dict(game)
#                 if game == False:
#                     continue
#                 moves = game.pop('moves_data')
#                 moves_format = format_one_game_moves(moves)
#                 if moves_format:
#                     moves_list.extend(moves_format)
#                 games_list.append(GameCreateData(**game).model_dump())
#                 # count+=1
#                 # if count%300==0:
#                 #     print(count)
#             print('time elapsed formating a month of games: ', time.time()-start, year, month)
#     start = time.time()
#     game_insert_players(games_list)
#     print(f'inserted players from : {len(games_list)} games', time.time()-start)
#     insert_new_data(games_list, moves_list, months_list)
#     print(f'inserted games: {len(games_list)}', time.time()-start)
    
#     return f"DONE DOWNLOADING AND INSERTING EVERY GAME FROM: {player_name}"
