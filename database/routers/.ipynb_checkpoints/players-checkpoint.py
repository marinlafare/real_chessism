#ROUTERS
from fastapi import APIRouter
from database.operations.players import insert_player, read_player
router = APIRouter()

@router.get("/player")
def api_read_game(player_name:dict) -> str:
    return read_player(player_name)

@router.post("/player")
def api_create_game(data:dict) -> str:
    player = insert_player(data)
    print(player)
    return player
