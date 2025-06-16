#OPERATIONS
from database.database.models import Player
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from .models import PlayerCreateData, PlayerResult
from database.database.db_interface import DBInterface
from .chess_com_api import get_profile
player_interface = DBInterface(Player)

def insert_player(data: dict)-> PlayerResult:
    if player_interface.player_exists(data['player_name'].lower()):
         return "player already exists at DB".upper()
    player_name = data['player_name']
    profile = get_profile(player_name)
    if type(profile)==str:
         return profile
    profile['player_name'] = player_name
    player_data = PlayerCreateData(**profile)
    player = player_interface.create(player_data.model_dump())
    player = jsonable_encoder(player)
    return JSONResponse(content=player)
def read_player(player_name):
     profile = player_interface.read_by_name(player_name['player_name'].lower())
     if not profile:
          return 'No player at DB'
     profile = jsonable_encoder(profile)
     return JSONResponse(content=profile)