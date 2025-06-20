#OPERATIONS

from database.database.models import Game
from .format_dates import just_new_months
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
    new_months = just_new_months(player_name)
    if not new_months:
        return 'ALL MONTHS IN DB ALREADY'
    print('DOWNLOADING')
    start_download = time.time()
    games = download_months(player_name,new_months, parallel=False)
    end_download = time.time()
    print('DOWNLOADED IN: ', (end_download-start_download)/60)
    format_and_insert_games(games, player_name)
    end_create_games = time.time()
    print('Format done in: ',(end_create_games-start_create_games)/60)
    return f"DATA READY FOR {player_name}"
    