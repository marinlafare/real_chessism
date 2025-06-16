#OPERATIONS
import numpy as np
from .models import PlayerCreateData, GameCreateData, MoveCreateData
import re
import pandas as pd
import multiprocessing as mp
import concurrent
from database.database.ask_db import player_exists_at_db, open_request
from database.database.db_interface import DBInterface
from database.database.models import Player, Game, Month, Move
player_interface = DBInterface(Player)
game_interface = DBInterface(Game)
month_interface = DBInterface(Month)
move_interface = DBInterface(Move)
def format_valid_range(valid_range):
    months = ''
    for date in valid_range:
        format = f'{date[0]}_{date[1]}###'
        months += format
    return months
def get_new_links(new_links):
    new = []
    for id_ in new_links:
        if len(
            open_request(f"select id from game where game.id = {id_}")
            )==1:
            continue
        new.append(id_)

    return new
def get_player_profile(params):
    player_name = params['player_name']    
    profile = {'player_name':player_name}
    profile = PlayerCreateData(**profile)
    params['profiles'].append(profile.model_dump())
def insert_players(new_players):
    print('II')
    profiles = mp.Manager().list()
    params = [
                {
                    "player_name": player_name,
                    "profiles": profiles,
                }
                for player_name in new_players
            ]
 
    print("Entering Pooling")
    pool = mp.Pool(2, maxtasksperchild=1)
    with pool:
        pool.map(get_player_profile, params)
    print("Out of the Pool")
    #print(profiles)
    if len(list(profiles)) == 0:
        return []
    return list(profiles)

def insert_new_players(new_players):
    new = []
    for player in new_players:
        if player_exists_at_db(player):
            continue
        else:
            new.append(player)
    if len(new)==0:
        print('ZERO NEW PLAYERS')
        return
    profiles = insert_players(new)
    print('len players',len(profiles))
    player_interface.create_all(profiles)
    print(f'{len(profiles)} NEW PLAYERS')
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

def clean_games(game: str, new_links: list[int]) -> bool:
    """
    It validates games for length of moves and formatting errors

    """
    game_with_more_than_n_moves = "10. "  # format: 'INT. '

    try:
        game.split("\n\n")[1][:-4]
    except:
        return False
    if not game_with_more_than_n_moves in game.split("\n\n")[1][:-4]:
        return False
    if game.split("\n\n")[1][:-4].startswith("1... "):
        return False
    if (
        "/"
        in game.split(f"TimeControl")[1]
        .split("\n")[0]
        .replace('"', "")
        .replace("]", "")
        .replace(" ", "")
        .lower()
    ):
        return False
    link = get_pgn_item(game, "[Link").split("/")[-1]
    if link not in new_links:
        print('link not in new')
        return False
    return True
def validate_games(games):

    players_and_game_id = np.array(
        [
            [
                get_pgn_item(game, "White").lower(),
                get_pgn_item(game, "Black").lower(),
                int(get_pgn_item(game, "[Link").split("/")[-1]),
            ]
            for game in games
        ]
    )
    print('############')
    print('players this month')
    print('###############')

    players_this_month = set(players_and_game_id[:, 0])
    black_players_this_month = set(players_and_game_id[:, 1])
    players_this_month.update(black_players_this_month)
    insert_new_players(players_this_month)
    new_links = get_new_links(players_and_game_id[:, 2])
    if len(new_links) == 0:
        return 'NO NEW GAMES'
    #print(new_links)
    print(f"GAMES BEFORE CLEANING = {len(games)}")
    games = [game for game in games if clean_games(game, new_links)]
    print(f"GAMES AFTER CLEANING = {len(games)}")    
    return games
def get_result(user_name, result):
    if "drawn" in result:
        return 0.5
    if user_name in result:
        return 1
    else:
        return 0


def get_time_elapsed(game):
    start = get_pgn_item(game, "Date") + " " + get_pgn_item(game, "StartTime")
    end = get_pgn_item(game, "EndDate") + " " + get_pgn_item(game, "EndTime")
    delta = pd.to_datetime(end) - pd.to_datetime(start)
    return str(delta).split()[-1]


def clean_termination(one_termination: str):
    if "drawn" in one_termination:
        return one_termination.replace("game drawn by ", "")
    if "time" in one_termination:
        return "time"
    if "abandoned" in one_termination:
        return "abandoned"
    if "checkmate" in one_termination:
        return "checkmate"
    if "resignation" in one_termination:
        return "resignation"
    return one_termination


def get_eco(game: str):
    try:
        return get_pgn_item(game, "ECO")
    except:
        return "no_eco"


def get_time_bonus(game):
    time_control = get_pgn_item(game, "TimeControl")
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
    times: list,
    clean_moves: list,
    n_moves: int,
    time_bonus: int,
    id_:int
) -> dict[str]:
    """
    it transfor the row moves and times_left into two columns
    it assing the moves to white and black
    calculates the times_left to get reaction times
    it return a dict with a single string
    """
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
        "id": id_,
        "moves": "".join([str(x) + " " for x in range(1, n_moves + 1)])[:-1],
        "white_moves": "".join([str(x) + " " for x in ordered_moves[:, 0]])[:-1],
        "white_reaction_times": "".join([str(x) + " " for x in white_cumsub])[:-1],
        "white_time_left": "".join([str(x) + " " for x in white_times])[:-1],
        "black_moves": "".join([str(x) + " " for x in ordered_moves[:, 1]])[:-1],
        "black_reaction_times": "".join([str(x) + " " for x in black_cumsub])[:-1],
        "black_time_left": "".join([str(x) + " " for x in black_times])[:-1],
    }
    return result
def get_moves_data(game: str) -> tuple:
    """
    calculates n_moves
    It separates the white and black moves,
    claculates reaction times for both players
    returns a dictionary with keys:
        moves:["1...n_moves"],
        moves_white["e4..."], moves_black["e5..."],
        white_reaction_times["0.1,0.5,1..."], black_reaction_times["0.1,0.2,0.1..."]
        white_time_left["59.9,59.4,58.4"], black_time_left["59.9,59.7,59.6"]

    """
    id_ = int(get_pgn_item(game, "[Link").split("/")[-1])
    time_bonus = get_time_bonus(game)
    raw_moves = (
        game.split("\n\n")[1]
        .replace("1/2-1/2", "")
        .replace("1-0", "")
        .replace("0-1", "")
    )
    n_moves = get_n_moves(raw_moves)
    times = [x.replace("]", "").replace("}", "") for x in raw_moves.split() if ":" in x]
    no_times = re.sub(r"{[^}]*}*", "", raw_moves)
    clean_moves = [x for x in no_times.split() if "." not in x]
    if not f"{n_moves}..." in raw_moves:
        clean_moves.append("-")
        times.append("-")
    moves_data = create_moves_table(times,
                                    clean_moves,
                                    n_moves,
                                    time_bonus,
                                    id_)
    return n_moves, moves_data
def get_games_classes(params: dict) -> None:
    """
    Interface to load the result of making one game_class creation
    returns foreign list.append() from read_games: mp.Pool
    """
    game_dict, moves_data = create_game_dict(params[0])
    params[1].append(game_dict)
    params[2].append(moves_data)

def create_game_dict(game: str) -> tuple:
    """
    Self explanatory
    """
    game_dict = dict()
    
    game_dict["id"] = int(get_pgn_item(game, "[Link").split("/")[-1])
    game_dict["start_time"] = get_pgn_item(game, "StartTime")
    game_dict["year"] = int(get_pgn_item(game, "Date")[:4])
    game_dict["month"] = int(get_pgn_item(game, "Date")[5:7])
    game_dict["day"] = int(get_pgn_item(game, "Date")[8:10])
    game_dict["white"] = get_pgn_item(game, "White").lower()
    game_dict["black"] = get_pgn_item(game, "Black").lower()
    game_dict["white_elo"] = int(get_pgn_item(game, "WhiteElo"))
    game_dict["black_elo"] = int(get_pgn_item(game, "BlackElo"))
    game_dict["white_result"] = float(get_result(
        get_pgn_item(game, "White").lower(), get_pgn_item(game, "Termination")
    ))
    game_dict["black_result"] = float(get_result(
        get_pgn_item(game, "Black").lower(), get_pgn_item(game, "Termination")
    ))
    game_dict["time_control"] = get_pgn_item(game, "TimeControl")
    game_dict["eco"] = get_eco(game)
    game_dict["time_elapsed"] = get_time_elapsed(game)
    game_dict["termination"] = clean_termination(get_pgn_item(game, "Termination"))
    n_moves, moves_data = get_moves_data(game)
    game_dict["n_moves"] = n_moves

    return game_dict, moves_data
def format_games(games):
    # games_list = mp.Manager().list()
    # moves_list = mp.Manager().list()
    games_list = []
    moves_list = []
    params = [(game, games_list, moves_list) for game in games]
    print("Entering GAME Pooling")
    # pool = mp.Pool(2, maxtasksperchild=1)
    # with pool:
    #     pool.map(get_games_classes, params)
    for param in params:
        get_games_classes(param)
    print("Out of the Pool")
    print("Out#########################")
    print(games_list[0])
    print("O############################")
    GameCreateData(**games_list[0])
    MoveCreateData(**moves_list[0])
    games_list = [game for game in games_list]
    moves_list = [moves for moves in moves_list]
    return games_list, moves_list

def insert_games(games, valid_range):
    valid_range = format_valid_range(valid_range)
    games = validate_games(games)
    games_list, moves_list = format_games(games)
    game_interface.create_all(games_list)
    move_interface.create_all(moves_list)

    return 'GAMES INSERTES AND MOVES AND WHAT NOT'

    