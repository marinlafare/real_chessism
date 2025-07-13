# database/routers/games.py
from fastapi.responses import JSONResponse
from fastapi import APIRouter, Body
from database.operations.games import create_games, read_game, update_player_games

from database.database.ask_db import get_principal_players
router = APIRouter()



@router.get("/games/{link}")
async def api_read_game(link: str) -> JSONResponse:
    """
    Retrieves game information by its link.

    Args:
        link (str): The string representation of the game ID (e.g., "138708864874").

    Returns:
        JSONResponse: JSON info of the game, or 404 if not found.
    """
    print(link)
    game = await read_game(link)
    return game
    try:
        # Assuming read_game expects the link directly
        game = await read_game(link)
        if game is None:
            raise HTTPException(status_code=404, detail=f"Game with link '{link}' not found.")
        return JSONResponse(content=game)
    except Exception as e:
        # Log the error for debugging
        print(f"Error fetching game {link}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

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

@router.post("/games/update/{player_name}")
async def api_update_player_games(data: dict = Body(...)) -> JSONResponse:
    congratulation = await update_player_games(player_name)
    return congratulation

@router.post("/games/update/all")
async def api_update_all_players_games() -> JSONResponse:
    players = await get_principal_players()
    for player in players:
        await update_player_games(player_name)
    return "EVERY PLAYER UPDATED"
