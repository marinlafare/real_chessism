# database/routers/games.py
from fastapi.responses import JSONResponse
from fastapi import APIRouter, Body # Import Body for explicit body parsing
from database.operations.games import create_games, read_game

router = APIRouter()

@router.get("/games/{link}")
async def api_read_game(data: dict = Body(...)) -> JSONResponse:
    """
    Arg: str representation of an int: 
                                      get("/games/123456")
                                      
    Returns: json info of the game
    """
    game = await read_game(data)
    return game

@router.post("/games/{player_name}")
async def api_create_game(data: dict = Body(...)) -> JSONResponse:
    """
    It fetches every available game from the user
    inserts them into DB.
    
    Arg: some user from chess.com: 
                                  get("/games/some_chesscom_user")
                                  
    Returns: a string congratulating you for having more games now.
    """
    congratulation = await create_games(data)
    return congratulation