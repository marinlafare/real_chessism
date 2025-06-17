#OPERATIONS
from fastapi.encoders import jsonable_encoder

import numpy as np
from constants import DRAW_RESULTS, LOSE_RESULTS, CONN_STRING
from .models import PlayerCreateData, GameCreateData, MoveCreateData, MonthCreateData
import re
import pandas as pd
import multiprocessing as mp
import concurrent
from database.database.ask_db import player_exists_at_db, open_request
from database.database.db_interface import DBInterface
from database.database.models import Player, Game, Month, Move
import time
from datetime import datetime
from database.operations.chess_com_api import get_profile




def insert_player(data: dict):
    player_interface = DBInterface(Player)
    if player_interface.player_exists(data['player_name'].lower()):
         return "player already exists at DB".upper()
    player_name = data['player_name']
    profile = get_profile(player_name)
    if type(profile)==str:
         return profile
    profile['player_name'] = player_name
    player_data = PlayerCreateData(**profile)
    player_interface.create(player_data.model_dump())
    return


def get_pgn_item(game, item: str) -> str:
    if item == "Termination":
        return (
            game.split(f"{item}")[1]
            .split("\n")[0]
            .replace('"', "")
            .replace("]", "")
            .lower()
        )
    return (
        game.split(f"{item}")[1]
        .split("\n")[0]
        .replace('"', "")
        .replace("]", "")
        .replace(" ", "")
        .lower()
    )



def get_start_and_end_date(game,game_for_db):
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
    DRAW_RESULTS, LOSE_RESULTS
    if str_result == 'win':
        return 1.0
    if str_result in DRAW_RESULTS:
        return 0.0
    if str_result in LOSE_RESULTS:
        return -1.0
    else:
        return str_result
def get_black_and_white_data(game, game_for_db):
    game_for_db['black'] = game['black']['username'].lower()
    game_for_db['black_elo'] = int(game['black']['rating'])
    game_for_db['black_str_result'] = game['black']['result'].lower()
    game_for_db['black_result'] = translate_result_to_float(game_for_db['black_str_result'])

    game_for_db['white'] = game['white']['username'].lower()
    game_for_db['white_elo'] = int(game['white']['rating'])
    game_for_db['white_str_result'] = game['white']['result'].lower()
    game_for_db['white_result'] = translate_result_to_float(game_for_db['white_str_result'])
    return game_for_db
def create_no_pgn_dict(game):
    game_for_db = dict()
    game_for_db['link'] = int(game['url'].split('/')[-1])
    game_for_db['time_control']  = game['time_control']
    game_for_db = get_black_and_white_data(game, game_for_db)
    game_for_db['year'] = 0
    game_for_db['month'] = 0
    game_for_db['day'] = 0
    game_for_db['hour'] =0
    game_for_db['minute'] =0
    game_for_db['second'] =0
    game_for_db['n_moves']  = 0
    game_for_db['moves_data']  = {"link":game['url'] }
    game_for_db['end_year'] = 0 
    game_for_db['end_month'] = 0
    game_for_db['end_day'] = 0
    game_for_db['end_hour'] = 0
    game_for_db['end_minute'] = 0
    game_for_db['end_second'] = 0
    game_for_db['time_elapsed'] = 0
    game_for_db['eco'] = 'no_eco'
    return game_for_db
def create_game_dict(game):
    try:
        game['pgn']
    except:
        return create_no_pgn_dict(game)
        
    game_for_db = dict()
    game_for_db['link'] = int(game['url'].split('/')[-1])
    game_for_db['time_control']  = game['time_control']
    game_for_db = get_start_and_end_date(game, game_for_db)
    game_for_db = get_black_and_white_data(game, game_for_db)
    n_moves, moves_data = get_moves_data(game)
    game_for_db['n_moves']  = n_moves
    game_for_db['moves_data']  = moves_data
    try:
        game_for_db['eco']  = game['eco']
    except:
        game_for_db['eco']  = 'no_eco'
    return game_for_db


def get_time_bonus(game):
    time_control = game['time_control']
    if "+" in time_control:
        return int(time_control.split("+")[-1])
    return 0
def get_n_moves(raw_moves):
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
        "white_moves": ordered_moves[:, 0],
        "white_reaction_times": white_cumsub,
        "white_time_left": white_times,
        "black_moves": ordered_moves[:, 1],
        "black_reaction_times": black_cumsub,
        "black_time_left": black_times
    }
    return result
def get_moves_data(game: str) -> tuple:
    time_bonus = get_time_bonus(game)
    #print(game.keys())
        
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
def format_one_game_moves(moves):
    to_insert_moves = []
    try:
        moves['white_moves']
    except:
        return False
    for ind, white_move in enumerate(moves['white_moves']):
        moves_dict = {}
        moves_dict['n_move'] = ind + 1
        moves_dict['link'] = moves['link']
        moves_dict['white_move'] = white_move
        moves_dict['black_move'] = moves['white_moves'][ind]
        moves_dict['white_reaction_time'] = round(moves['white_reaction_times'][ind],3)
        moves_dict['white_time_left'] = round(moves['white_time_left'][ind],3)

        try:
            moves_dict['black_reaction_time'] = round(moves['black_reaction_times'][ind],3)
            moves_dict['black_time_left'] = round(moves['black_time_left'][ind],3)
        except:
            moves_dict['black_reaction_time'] = 0.0
            moves_dict['black_time_left'] = 0.0
        to_insert_moves.append(MoveCreateData(**moves_dict).model_dump())
    return to_insert_moves
def insert_players(games_list):
    whites = [x['white'] for x in games_list]
    blacks = [x['black'] for x in games_list]
    players_set = set(whites)
    players_set.update(set(blacks))
    for player in players_set:
        print(player)
        inserted_player = insert_player({"player_name":player})
        if type(inserted_player) == str:
            print(player)
            print(inserted_player)
def format_and_insert_games(games, player_name):
    games_list = []
    moves_list = []
    months_list = []
    for year in games.keys():
        for month in games[year].keys():
            month_data = {"player_name":player_name,
                         "year":year,
                         "month":month,
                         "n_games":len(games[year][month])}
            months_list.append(MonthCreateData(**month_data).model_dump())
            for game in games[year][month]:
                game = create_game_dict(game)
                moves = game.pop('moves_data')
                moves_format = format_one_game_moves(moves)
                if moves_format:
                    moves_list.extend(moves_format)
                games_list.append(GameCreateData(**game).model_dump())
    insert_players(games_list)
                
    return games_list, moves_list, months_list


# from database.database.engine import init_db
# init_db(CONN_STRING)

# player_interface = DBInterface(Player)
# game_interface = DBInterface(Game)
# month_interface = DBInterface(Month)
# move_interface = DBInterface(Move)


# def format_valid_range(player_name, valid_range):
#     months = ''
#     for date in valid_range:
#         format = f'{date[0]}_{date[1]}###'
#         months += format
#     months = month_interface.read_by_name(player_name)
#     print(months)
#     return months

# def get_player_profile(params):
#     player_name = params['player_name']    
#     profile = {'player_name':player_name}
#     profile = PlayerCreateData(**profile)
#     params['profiles'].append(profile.model_dump())
# def insert_players(new_players):
#     print('II')
#     profiles = mp.Manager().list()
#     params = [
#                 {
#                     "player_name": player_name,
#                     "profiles": profiles,
#                 }
#                 for player_name in new_players
#             ]
 
#     print("Entering Pooling")
#     pool = mp.Pool(2, maxtasksperchild=1)
#     with pool:
#         pool.map(get_player_profile, params)
#     print("Out of the Pool")
#     #print(profiles)
#     if len(list(profiles)) == 0:
#         return []
#     return list(profiles)

# def insert_new_players(new_players):
#     to_insert = []
#     for player in new_players:
#         if player_exists_at_db(player):
#             continue
#         else:
#             profile = {'player_name':player}
#             profile = PlayerCreateData(**profile).model_dump()
#             to_insert.append(profile)
#     player_interface.create_all(to_insert)
#     return

# def clean_games(game: str) -> bool:
#     """
#     It validates games for length of moves and formatting errors

#     """
#     link = int(get_pgn_item(game, "[Link").split("/")[-1])
#     if not len(
#             open_request(f"select id from game where game.id = {link}")
#             )==0:
#         return False
#     game_with_more_than_n_moves = "10. "  # format: 'INT. '

#     try:
#         game.split("\n\n")[1][:-4]
#     except:
#         return False
#     if not game_with_more_than_n_moves in game.split("\n\n")[1][:-4]:
#         return False
#     if game.split("\n\n")[1][:-4].startswith("1... "):
#         return False
#     if (
#         "/"
#         in game.split(f"TimeControl")[1]
#         .split("\n")[0]
#         .replace('"', "")
#         .replace("]", "")
#         .replace(" ", "")
#         .lower()
#     ):
#         return False
#     return True
# def validate_games(games):

#     players_and_game_id = np.array(
#         [
#             [
#                 get_pgn_item(game, "White").lower(),
#                 get_pgn_item(game, "Black").lower(),
#                 int(get_pgn_item(game, "[Link").split("/")[-1]),
#             ]
#             for game in games
#         ]
#     )

#     players_this_month = set(players_and_game_id[:, 0])
#     black_players_this_month = set(players_and_game_id[:, 1])
#     players_this_month.update(black_players_this_month)
#     insert_new_players(players_this_month)
    
#     print(f"GAMES BEFORE CLEANING = {len(games)}")
#     start_games_clean = time.time()
#     games = [game for game in games if clean_games(game)]
#     end_games_clean = time.time()
#     print((end_games_clean-start_games_clean)/60, ' minutes')
#     print(f"GAMES AFTER CLEANING = {len(games)}")
#     if len(games) == 0:
#         return 'NO NEW GAMES'
#     return games
# def get_result(user_name, result):
#     if "drawn" in result:
#         return 0.5
#     if user_name in result:
#         return 1
#     else:
#         return 0

# def get_time_elapsed(game):
#     start = get_pgn_item(game, "Date") + " " + get_pgn_item(game, "StartTime")
#     end = get_pgn_item(game, "EndDate") + " " + get_pgn_item(game, "EndTime")
#     delta = pd.to_datetime(end) - pd.to_datetime(start)
#     return str(delta).split()[-1]

# def clean_termination(one_termination: str):
#     if "drawn" in one_termination:
#         return one_termination.replace("game drawn by ", "")
#     if "time" in one_termination:
#         return "time"
#     if "abandoned" in one_termination:
#         return "abandoned"
#     if "checkmate" in one_termination:
#         return "checkmate"
#     if "resignation" in one_termination:
#         return "resignation"
#     return one_termination


# def get_eco(game: str):
#     try:
#         return get_pgn_item(game, "ECO")
#     except:
#         return "no_eco"

# def get_time_bonus(game):
#     time_control = get_pgn_item(game, "TimeControl")
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
#     times: list,
#     clean_moves: list,
#     n_moves: int,
#     time_bonus: int,
#     id_:int
# ) -> dict[str]:
#     """
#     it transfor the row moves and times_left into two columns
#     it assing the moves to white and black
#     calculates the times_left to get reaction times
#     it return a dict with a single string
#     """
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
#         "id": id_,
#         "moves": "".join([str(x) + " " for x in range(1, n_moves + 1)])[:-1],
#         "white_moves": "".join([str(x) + " " for x in ordered_moves[:, 0]])[:-1],
#         "white_reaction_times": "".join([str(x) + " " for x in white_cumsub])[:-1],
#         "white_time_left": "".join([str(x) + " " for x in white_times])[:-1],
#         "black_moves": "".join([str(x) + " " for x in ordered_moves[:, 1]])[:-1],
#         "black_reaction_times": "".join([str(x) + " " for x in black_cumsub])[:-1],
#         "black_time_left": "".join([str(x) + " " for x in black_times])[:-1],
#     }
#     return result
# def get_moves_data(game: str) -> tuple:
#     """
#     calculates n_moves
#     It separates the white and black moves,
#     claculates reaction times for both players
#     returns a dictionary with keys:
#         moves:["1...n_moves"],
#         moves_white["e4..."], moves_black["e5..."],
#         white_reaction_times["0.1,0.5,1..."], black_reaction_times["0.1,0.2,0.1..."]
#         white_time_left["59.9,59.4,58.4"], black_time_left["59.9,59.7,59.6"]

#     """
#     id_ = int(get_pgn_item(game, "[Link").split("/")[-1])
#     time_bonus = get_time_bonus(game)
#     raw_moves = (
#         game.split("\n\n")[1]
#         .replace("1/2-1/2", "")
#         .replace("1-0", "")
#         .replace("0-1", "")
#     )
#     n_moves = get_n_moves(raw_moves)
#     times = [x.replace("]", "").replace("}", "") for x in raw_moves.split() if ":" in x]
#     no_times = re.sub(r"{[^}]*}*", "", raw_moves)
#     clean_moves = [x for x in no_times.split() if "." not in x]
#     if not f"{n_moves}..." in raw_moves:
#         clean_moves.append("-")
#         times.append("-")
#     moves_data = create_moves_table(times,
#                                     clean_moves,
#                                     n_moves,
#                                     time_bonus,
#                                     id_)
#     return n_moves, moves_data
# def get_games_classes(params: dict) -> None:
#     """
#     Interface to load the result of making one game_class creation
#     returns foreign list.append() from read_games: mp.Pool
#     """
#     game_dict, moves_data = create_game_dict(params[0])
#     params[1].append(game_dict)
#     params[2].append(moves_data)

# def create_game_dict(game: str) -> tuple:
#     """
#     Self explanatory
#     """
#     game_dict = dict()
    
#     game_dict["id"] = int(get_pgn_item(game, "[Link").split("/")[-1])
#     game_dict["start_time"] = get_pgn_item(game, "StartTime")
#     game_dict["year"] = int(get_pgn_item(game, "Date")[:4])
#     game_dict["month"] = int(get_pgn_item(game, "Date")[5:7])
#     game_dict["day"] = int(get_pgn_item(game, "Date")[8:10])
#     game_dict["white"] = get_pgn_item(game, "White").lower()
#     game_dict["black"] = get_pgn_item(game, "Black").lower()
#     game_dict["white_elo"] = int(get_pgn_item(game, "WhiteElo"))
#     game_dict["black_elo"] = int(get_pgn_item(game, "BlackElo"))
#     game_dict["white_result"] = float(get_result(
#         get_pgn_item(game, "White").lower(), get_pgn_item(game, "Termination")
#     ))
#     game_dict["black_result"] = float(get_result(
#         get_pgn_item(game, "Black").lower(), get_pgn_item(game, "Termination")
#     ))
#     game_dict["time_control"] = get_pgn_item(game, "TimeControl")
#     game_dict["eco"] = get_eco(game)
#     game_dict["time_elapsed"] = get_time_elapsed(game)
#     game_dict["termination"] = clean_termination(get_pgn_item(game, "Termination"))
#     n_moves, moves_data = get_moves_data(game)
#     game_dict["n_moves"] = n_moves

#     return game_dict, moves_data
# def format_games(games):
#     games_list = []
#     moves_list = []
#     for game in games:
#         game_dict, moves_data = create_game_dict(game)
#         games_list.append(GameCreateData(**game_dict).model_dump())
#         moves_list.append(MoveCreateData(**moves_data).model_dump())

#     game_interface.create_all(games_list)
#     move_interface.create_all(moves_list)

# def old_insert_games(games):
#     print('VALID GAMES #############')
#     start = time.time()
#     games = validate_games(games)
#     end = time.time()
#     print(f'VALIDATED ELAPSED IN: {(end-start)/60}')
#     if type(games) == str:
#         return games
#     print('FORMAT GAMES #############')
#     start = time.time()
#     format_games(games)
#     end = time.time()
#     print(f'FORMATED: {(end-start)/60}')
#     return 'GAMES INSERTS AND MOVES AND WHAT NOT'
# def insert_games(games):
#     pass

################################
################################
################################