#ROUTERS
from fastapi import APIRouter
from database.operations.games import create_games, read_game
router = APIRouter()

@router.get("/games")
def api_read_game(data:dict) -> str:
    return read_game(data)
@router.post("/games")
def api_create_game(data:dict) -> str:
    return create_games(data)