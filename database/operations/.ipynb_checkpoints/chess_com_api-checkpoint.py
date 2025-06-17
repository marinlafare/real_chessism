#OPERATIONS

import os
from dotenv import load_dotenv
import requests
import json
import io
import concurrent
import time
from constants import *
import database.operations.chess_com_endpoints as chess_com_endpoints

# CHESSCOM ENDPOINTS


#
def get_profile(player_name):
    PLAYER = chess_com_endpoints.PLAYER.replace('{player}', player_name)
    response = requests.get(PLAYER,
                            timeout=3,
                            headers = {"User-Agent": chess_com_endpoints.USER_AGENT})
    
    if response.status_code == 200:
        response = response.__dict__["_content"].decode("utf-8")
        response = json.loads(response)
        response.pop('@id')
        response.pop('username')
        response.pop('last_online')
        country = response.pop('country').split('/')[-1]
        response['country'] = country
        if "message" in response:
            return False
        return response
    else:
        return f'RESPONSE {response.status_code}'
def ask_twice(player_name: str, year: int, month: int):
    USER_AGENT = chess_com_endpoints.USER_AGENT
    DOWNLOAD_MONTH = (
        chess_com_endpoints.DOWNLOAD_MONTH
        .replace("{player}", player_name)
        .replace("{year}", str(year))
        .replace("{month}", str(month))
    )
    games = requests.get(DOWNLOAD_MONTH,
                         allow_redirects=True,
                         timeout=3,
                         headers = {"User-Agent": USER_AGENT})
    if len(games.content) == 0:
        time.sleep(1)
        games = requests.get(DOWNLOAD_MONTH,
                             allow_redirects=True,
                             timeout=10,
                             headers = {"User-Agent": USER_AGENT})
    if len(games.content) == 0:
        return False
    return games
def download_month(player_name: str, year: int, month: int) -> str:
    """It ask for a month of games of a particular player
    the games package is a pgn_txt.
    Returns:
        io.str
    """
    print(year, month)
    games = ask_twice(player_name, year, month)
    if games == False:
        return False
    pgn = io.StringIO(games.content.decode().replace("'", '"'))
    return pgn
def month_of_games(params: list, parallel = True) -> None:
    """
    Download a month, splits the games and return a list of pgn games.
    """
    pgn = download_month(params["player_name"], params["year"], params["month"])
    if pgn == False:
        if not parallel:
            return False
        return params["return_games"].append((False,False,False))
    if "\n\n\n" in pgn.read():
        if not parallel:
            return pgn.read().split("\n\n\n")
        params["return_games"].append((params["year"],params["month"],pgn.read().split("\n\n\n")))
    else:
        pgn = pgn.getvalue()
        pgn = json.loads(pgn)
        if not parallel:
            return pgn['games'] 
        params["return_games"].append((params["year"],params["month"],pgn['games']))
        
def download_months(player_name, valid_dates, parallel=False):
    """
    Ask chessdotcom for the games on the date_range trough a threadpool
    returns valid games for valid months and a list of months asked
    """
    return_games = []
    params = [
        {
            "player_name": player_name,
            "year": date[0],
            "month": date[1],
            "return_games": return_games,
        }
        for date in valid_dates
    ]
    if parallel:
        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            executor.map(month_of_games, params)
        print(f"GOT {len(return_games)} games")
        print("downloading over")
        return_games = [game for game in return_games if game is not False]
        print(f'returned games = {len(return_games)}')

        years = set([x[0] for x in return_games])
        years = {name: {} for name in years}
        
        for year_month_game in return_games:
            year = year_month_game[0]
            month = year_month_game[1]
            try:
                years[year][month]
            except:
                years[year][month] = list()
            years[year][month].extend(year_month_game[2])
        return years
    else:
        years = set([x["year"] for x in params])
        games = dict()
        for year in years:
            games[year] = dict()
        for param in params:
            games[param["year"]][param["month"]] = list()
            param["return_games"] = []
            pgn = month_of_games(param, parallel = False)
            for game in pgn:
                games[param["year"]][param["month"]].append(game)
        return games