#OPERATIONS

from database.database.models import Game
from .format_dates import create_range, just_new_dates
from .format_games import format_and_insert_games
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from database.database.db_interface import DBInterface
from .chess_com_api import download_months
import time

game_interface = DBInterface(Game)

def read_game(data):
    player = data
    player = jsonable_encoder(player)
    return JSONResponse(content=player)
    
def create_games(data:dict)->str:
    player_name = data['player_name'].lower()
    start_create_games = time.time()
    date_range = create_range(data)
    if type(date_range) == str:
        date_range = jsonable_encoder(date_range)
        return date_range
    #print('data range',date_range)
    valid_range = just_new_dates(player_name, date_range)
    #print(valid_range)
    if type(valid_range)==str:
        return valid_range
    print('DOWNLOADING')
    start_download = time.time()
    games = download_months(player_name,valid_range)
    end_download = time.time()
    print('DOWNLOADED IN ', (end_download-start_download)/60)
    format_and_insert_games(games)
    print('DONEEEEEEEE')

    end_create_games = time.time()

    print('CREATE GAMES',(end_create_games-start_create_games)/60)
    player = jsonable_encoder(valid_range)
    return JSONResponse(content=player)
    