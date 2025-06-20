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
    if len(str(month)) == 1:
        month = '0'+str(month)
    USER_AGENT = chess_com_endpoints.USER_AGENT
    DOWNLOAD_MONTH = (
        chess_com_endpoints.DOWNLOAD_MONTH
        .replace("{player}", player_name)
        .replace("{year}", str(year))
        .replace("{month}", str(month))
    )
    try:
        
        games = requests.get(DOWNLOAD_MONTH,
                             allow_redirects=True,
                             timeout=5,
                             headers = {"User-Agent": USER_AGENT})
        if len(games.content) == 0:
            time.sleep(1)
            games = requests.get(DOWNLOAD_MONTH,
                                 allow_redirects=True,
                                 timeout=10,
                                 headers = {"User-Agent": USER_AGENT})
        if len(games.content) == 0:
            return False
    except:
        return False
    return games
def download_month(player_name: str, year: int, month: int) -> str:
    """It ask for a month of games of a particular player
    the games package is a pgn_txt.
    Returns:
        io.str
    """
    games = ask_twice(player_name, year, month)
    if games == False:
        return False
    pgn = io.StringIO(games.content.decode().replace("'", '"'))
    return pgn
def month_of_games(params: list, parallel = True) -> None:
    count = 0
    """
    Download a month, splits the games and return a list of pgn games.
    """
    pgn = download_month(params["player_name"], params["year"], params["month"])
    if pgn == False:
        return False
    return pgn


    
    # if pgn == False:
    #     if not parallel:
    #         return False
    #     return params["return_games"].append((False,False,False))
    # if "\n\n\n" in pgn.read():
    #     if not parallel:
    #         return pgn.read().split("\n\n\n")
    #     params["return_games"].append((params["year"],params["month"],pgn.read().split("\n\n\n")))
    
    # else:
    #     text_games = pgn.getvalue()
    #     text_games = text_games.replace(' \\"Let"s Play!','lets_play')
    #     if text_games == False:
    #         return False
    #     try:
    #         pgn = json.loads(text_games)
    #     except:
    #         return text_games
    #     # if r"""[Event \"Let"s Play!\"]""" in pgn.getvalue():
    #     #     count+=1
    #     #     print(count)
    #     #     pgn = pgn.getvalue().replace(r"""[Event \"Let"s Play!\"]""", r"""[Event \"Lets Play!\"]""")
    #     #     print(pgn)
    #     #     pgn = json.loads(pgn)
    #     # else:
    #     #     pgn = pgn.getvalue()
    #     #     if "message" in pgn:
    #     #         return False
    #     #     try:
    #     #         pgn = json.loads(pgn)
    #     #     except:
    #     #         print('$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$')
    #     #         print(pgn)
    #     #         print('$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$')
    #     if 'code' in pgn.keys():
    #         return False
    #     if not parallel:
    #         return False
    #     params["return_games"].append((params["year"],params["month"],pgn['games']))
def filter_raw_pgn(pgn, year, month):
    if not pgn:
        return False
    if 'code' and 'message' in pgn:
        return False
    text_games = pgn.getvalue()
    text_games = text_games.replace(' \\"Let"s Play!','lets_play')
    try:
        return json.loads(text_games)
    except:
        print('string failed to create a JSON: ', f'year: {year}', f'month {month}')
        return False
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
            pgn = filter_raw_pgn(pgn, param["year"],param["month"])
            if pgn == False:
                print('no pgn ', param["year"],param["month"])
                continue
            for game in pgn['games']:
                games[param["year"]][param["month"]].append(game)
            #games[param["year"]][param["month"]].append(pgn)
        return games