#ROUTERS
from fastapi import APIRouter
from fastapi.responses import JSONResponse
from database.operations.players import insert_player, read_player
router = APIRouter()

@router.get("/player/{player_name}")
def api_read_player_name(player_name:str) -> str:
    profile = read_player(player_name)
    return JSONResponse(content=profile)
@router.get("/player/{player_name}/{feature}")
def api_read_player_feature(player_name:str, feature:str) -> str:
    profile = read_player(player_name)
    return JSONResponse(content= profile[feature])

@router.post("/player")
def api_create_player_profile(data:dict) -> str:
    player = insert_player(data)
    return player
