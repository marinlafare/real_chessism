# OPERATIONS FORMAT GAMES (REVISED for Asynchronous Execution)

from fastapi.encoders import jsonable_encoder

import numpy as np
from constants import DRAW_RESULTS, LOSE_RESULTS, WINING_RESULT, CONN_STRING
from .models import PlayerCreateData, GameCreateData, MoveCreateData, MonthCreateData
import re
import pandas as pd
import multiprocessing as mp # Keep for ThreadPoolExecutor for CPU-bound tasks
import concurrent # Keep for ThreadPoolExecutor for CPU-bound tasks
from database.database.ask_db import (
    get_players_already_in_db,
    get_games_already_in_db
) # No need for player_exists_at_db or open_request if handled by DBInterface
from database.database.db_interface import DBInterface
from database.database.models import Player, Game, Month, Move
import time
from datetime import datetime
from database.operations.chess_com_api import get_profile

# Define a global ThreadPoolExecutor for CPU-bound tasks, if needed.
# This should be managed carefully, e.g., using FastAPI's lifespan events.
# For simplicity, we'll create it here for now.
# cpu_executor = concurrent.futures.ThreadPoolExecutor(max_workers=mp.cpu_count())


# --- ASYNC FUNCTIONS ---
async def insert_player(data: dict):
    player_interface = DBInterface(Player) # Assuming DBInterface is now async-ready
    player_name = data['player_name'].lower()

    # Check if player exists asynchronously
    if await player_interface.player_exists(player_name):
        return await player_interface.read_by_name(player_name)

    profile = await get_profile(player_name) # get_profile is already async
    
    if profile is None: # get_profile returns None on error or not found
        print(f"Failed to fetch or process profile for {player_name}. Cannot insert.")
        return None # Return None or raise an appropriate exception

    # Ensure profile is a dict before trying to access keys, or handle Pydantic model directly
    if isinstance(profile, PlayerCreateData):
        profile_data = profile.model_dump()
    else: # This path should ideally not be taken if get_profile returns PlayerCreateData or None
        profile_data = profile # If get_profile ever returns a raw dict, handle it

    profile_data['player_name'] = player_name # Ensure consistency
    player_data = PlayerCreateData(**profile_data) # Re-validate if coming from raw dict

    await player_interface.create(player_data.model_dump())
    return jsonable_encoder(player_data)


# These functions are CPU-bound, they can remain synchronous or be wrapped in run_in_executor
# if they become a bottleneck and you want to use the event loop for other things.
# For now, let's keep them sync and consider `run_in_executor` for `format_one_game_moves`
# or the entire game processing loop within format_and_insert_games.

def get_pgn_item(game_pgn: str, item: str) -> str:
    # ... (no change, this is sync) ...
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
    # ... (no change, this is sync) ...
    try:
        game_date = get_pgn_item(game['pgn'], item='Date').split('.')
    except:
        game_for_db['year'] = 0
        return game_for_db
        
    game_for_db['year'] = int(game_date[0])
    game_for_db['month'] = int(game_date[1])
    game_for_db['day'] = int(game_date[2])
    
    game_start = get_pgn_item(game['pgn'], item='StartTime').split(':')
    game_for_db['hour'] =  int(game_start[0])
    game_for_db['minute'] = int(game_start[1])
    game_for_db['second'] = int(game_start[2])

    game_start = datetime(year = game_for_db['year'],
                          month = game_for_db['month'],
                          day = game_for_db['day'],
                          hour = game_for_db['hour'],
                          minute = game_for_db['minute'],
                          second = game_for_db['second'])
    
    game_end_date = get_pgn_item(game['pgn'], item='EndDate').split('.')
    game_for_db['end_year'] = int(game_end_date[0])
    game_for_db['end_month'] = int(game_end_date[1])
    game_for_db['end_day'] = int(game_end_date[2])
    game_end_time = get_pgn_item(game['pgn'], item='EndTime').split(':')
    game_for_db['end_hour'] = int(game_end_time[0])
    game_for_db['end_minute'] = int(game_end_time[1])
    game_for_db['end_second'] = int(game_end_time[2])

    game_end = datetime(
                        year= game_for_db['end_year'],
                        month = game_for_db['end_month'],
                        day = game_for_db['end_day'],
                        hour = game_for_db['end_hour'],
                        minute = game_for_db['end_minute'],
                        second =game_for_db['end_second']
    )

    game_for_db['time_elapsed'] = (game_end - game_start).seconds
    return game_for_db

def translate_result_to_float(str_result):
    # ... (no change, this is sync) ...
    if str_result in WINING_RESULT:
        return 1.0
    if str_result in DRAW_RESULTS:
        return 0.0
    if str_result in LOSE_RESULTS:
        return -1.0
    else:
        print('""""""UNKNOWN Natural Lenguague Result"""""""""""""')
        print(str_result)
        return str_result

def get_black_and_white_data(game, game_for_db):
    # ... (no change, this is sync) ...
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
    # ... (no change, this is sync) ...
    time_control = game['time_control']
    if "+" in time_control:
        return int(time_control.split("+")[-1])
    return 0

def get_n_moves(raw_moves):
    # ... (no change, this is sync) ...
    return max(
        [
            int(x.replace(".", ""))
            for x in raw_moves.split()
            if x.replace(".", "").isnumeric()
        ]
    )

def create_moves_table(
        game_url:str,
        times: list,
        clean_moves: list,
        n_moves: int,
        time_bonus: int) -> dict[str]:
    # ... (no change, this is sync) ...
    ordered_times = np.array(times).reshape((-1, 2))
    ordered_moves = np.array(clean_moves).reshape((-1, 2))
    white_times = pd.Series(
        [
            pd.Timedelta(str(str_time)).total_seconds()
            for str_time in ordered_times[:, 0]
        ]
    )
    black_times = pd.Series(
        [
            pd.Timedelta(str(str_time)).total_seconds()
            for str_time in ordered_times[:, 1]
            if str_time != "-"
        ]
    )
    white_cumsub = white_times.sub(white_times.shift(-1)) + time_bonus
    black_cumsub = black_times.sub(black_times.shift(-1)) + time_bonus

    result = {
        "link": int(game_url.split('/')[-1]),
        "white_moves": ordered_moves[:, 0].tolist(), # Convert numpy array to list for Pydantic
        "white_reaction_times": white_cumsub.tolist(),
        "white_time_left": white_times.tolist(),
        "black_moves": ordered_moves[:, 1].tolist(),
        "black_reaction_times": black_cumsub.tolist(),
        "black_time_left": black_times.tolist()
    }
    return result

def get_moves_data(game: dict) -> tuple: # Changed game type hint to dict
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
    clean_moves = [x for x in just_moves.split() if "." not in x]
    if not f"{n_moves}..." in raw_moves:
        clean_moves.append("-")
        times.append("-")
    moves_data = create_moves_table(game['url'],
                                    times,
                                    clean_moves,
                                    n_moves,
                                    time_bonus,
                                    )
    return n_moves, moves_data

def create_game_dict(game_raw_data: dict): # Renamed arg for clarity
    try:
        game_raw_data['pgn']
    except KeyError: # More specific exception
        return "NO PGN"
            
    game_for_db = dict()
    game_for_db['link'] = int(game_raw_data['url'].split('/')[-1])
    game_for_db['time_control']  = game_raw_data['time_control']
    game_for_db = get_start_and_end_date(game_raw_data, game_for_db)
    game_for_db = get_black_and_white_data(game_raw_data, game_for_db)
    try:
        n_moves, moves_data = get_moves_data(game_raw_data) # Pass raw game dict
    except Exception as e: # Catch specific errors or log more
        print(f"Error getting moves data for game {game_raw_data.get('url', 'N/A')}: {e}")
        return False
            
    game_for_db['n_moves']  = n_moves
    game_for_db['moves_data']  = moves_data
    try:
        game_for_db['eco']  = game_raw_data['eco']
    except KeyError: # More specific
        game_for_db['eco']  = 'no_eco'
    return game_for_db

def format_one_game_moves(moves: dict): # Type hint
    to_insert_moves = []
    try:
        moves['white_moves']
    except KeyError:
        return False
    for ind, white_move in enumerate(moves['white_moves']):
        moves_dict = {}
        moves_dict['n_move'] = ind + 1
        moves_dict['link'] = moves['link']
        
        moves_dict['white_move'] = str(white_move) # Ensure string conversion
        moves_dict['white_reaction_time'] = round(moves['white_reaction_times'][ind],3)
        moves_dict['white_time_left'] = round(moves['white_time_left'][ind],3)

        try:
            moves_dict['black_move'] = str(moves['black_moves'][ind]) # Ensure string conversion
            moves_dict['black_reaction_time'] = round(moves['black_reaction_times'][ind],3)
            moves_dict['black_time_left'] = round(moves['black_time_left'][ind],3)
        except IndexError: # If black move doesn't exist for this index (e.g., last move)
            moves_dict['black_move'] = '--'
            moves_dict['black_reaction_time'] = 0.0
            moves_dict['black_time_left'] = 0.0
        
        to_insert_moves.append(MoveCreateData(**moves_dict).model_dump())
    return to_insert_moves

# --- ASYNC DATABASE INTERACTION FUNCTIONS ---

async def game_insert_players(games_list):
    player_interface = DBInterface(Player)
    whites = [x['white'] for x in games_list]
    blacks = [x['black'] for x in games_list]
    players_set = set(whites)
    players_set.update(set(blacks))
    
    if not players_set: # Handle empty set
        print('no new player, this is wrong, this is very wrong')
        return

    in_db_players = await get_players_already_in_db(tuple(players_set)) # AWAIT
    to_insert_players = players_set - in_db_players
    
    if to_insert_players:
        players_data = [PlayerCreateData(**{"player_name":x}).model_dump() for x in to_insert_players]
        await player_interface.create_all(players_data) # AWAIT

async def get_just_new_games(games):
    links = []
    for year in games.keys():
        for month in games[year].keys():
            for game in games[year][month]:
                try:
                    # Check for 'pgn' and 'url' before appending
                    if 'pgn' in game and 'url' in game:
                        links.append(game['url'].split('/')[-1])
                except Exception as e:
                    print(f"Error extracting link from game: {e}, game data: {game}")
                    continue
    
    if not links:
        return False # No links to check

    in_db_games = await get_games_already_in_db(tuple(links)) # AWAIT
    to_insert_game_links = set([int(x) for x in links]) - in_db_games
    
    if not to_insert_game_links:
        return False
        
    new_games_structured = {}
    count = 0
    for year in games.keys():
        new_games_structured[year] = {}
        for month in games[year].keys():
            new_games_structured[year][month] = []
            for game in games[year][month]:
                if 'url' in game and int(game['url'].split('/')[-1]) in to_insert_game_links:
                    new_games_structured[year][month].append(game)
                    count += 1
    
    if count == 0:
        return False
    return new_games_structured

async def insert_new_data(games_list, moves_list, months_list):
    move_interface = DBInterface(Move)
    game_interface = DBInterface(Game)
    month_interface = DBInterface(Month)
    
    # Use asyncio.gather to run insertions concurrently if your DBInterface supports it
    await asyncio.gather(
        game_interface.create_all(games_list),
        month_interface.create_all(months_list),
        move_interface.create_all(moves_list)
    )

# --- MAIN FORMAT AND INSERT FUNCTION ---

async def format_and_insert_games(games, player_name):
    # Step 1: Filter out games already in DB (async)
    start_filter = time.time()
    games_to_process = await get_just_new_games(games)
    print(f"Filtered new games in: {time.time() - start_filter:.2f} seconds")

    if not games_to_process or len(games_to_process) == 0:
        return "All games already at DB"
    
    games_list_for_db = []
    moves_list_for_db = []
    months_list_for_db = []

    # Step 2: Format games (CPU-bound)
    start_format = time.time()
    for year in games_to_process.keys():
        for month in games_to_process[year].keys():
            month_data = {
                "player_name": player_name,
                "year": year,
                "month": month,
                "n_games": len(games_to_process[year][month])
            }
            months_list_for_db.append(MonthCreateData(**month_data).model_dump())

            # Consider using run_in_executor for this inner loop if it's truly a bottleneck
            # For now, it's synchronous within this async function, but it's CPU-bound.
            for game_raw_data in games_to_process[year][month]:
                game_dict = create_game_dict(game_raw_data)
                if game_dict == False or game_dict == "NO PGN":
                    # print(f"Skipping problematic game: {game_raw_data.get('url', 'N/A')}")
                    continue # Skip problematic games
                
                moves_data = game_dict.pop('moves_data')
                moves_formatted = format_one_game_moves(moves_data)
                
                if moves_formatted:
                    moves_list_for_db.extend(moves_formatted)
                
                games_list_for_db.append(GameCreateData(**game_dict).model_dump())
    print(f"Formatted {len(games_list_for_db)} games in: {time.time() - start_format:.2f} seconds")

    # Step 3: Insert data into DB (I/O-bound, run concurrently)
    start_insert = time.time()
    
    # First, insert players (can be done concurrently if player_insert_players is async)
    await game_insert_players(games_list_for_db) # AWAIT this
    print(f'Inserted players from {len(games_list_for_db)} games in: {time.time()-start_insert:.2f} seconds')

    # Then, insert games, moves, and months concurrently
    await insert_new_data(games_list_for_db, moves_list_for_db, months_list_for_db)
    print(f'Inserted games, moves, and months: {len(games_list_for_db)} games in: {time.time()-start_insert:.2f} seconds')
    
    return f"DONE DOWNLOADING AND INSERTING EVERY GAME FROM: {player_name}"


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
# def get_just_new_games(games):
    
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
