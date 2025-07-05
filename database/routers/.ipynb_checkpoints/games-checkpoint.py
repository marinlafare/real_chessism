# database/routers/games.py
from fastapi.responses import JSONResponse
from fastapi import APIRouter, Body # Import Body for explicit body parsing
from database.operations.games import create_games, read_game

router = APIRouter()



@router.get("/games/{link}")
async def api_read_game(link: str) -> JSONResponse: # <--- Change here: directly use 'link' from path
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

# # Placeholder for read_game for demonstration purposes
# # In your actual code, ensure this is correctly imported or defined.
# async def read_game(game_link: str):
#     """Mocks reading game data from a database."""
#     print(f"Reading game with link: {game_link}")
#     # Simulate a database lookup
#     if game_link == "138708864874":
#         return {"link": game_link, "player_white": "PlayerA", "player_black": "PlayerB", "moves": "e4 e5 Nf3 Nc6"}
#     if game_link == "123456":
#         return {"link": game_link, "player_white": "Hikaru", "player_black": "Magnus", "moves": "d4 d5 c4 c5"}
#     return None # Game not found

# @router.get("/games/{link}")
# async def api_read_game(data: dict = Body(...)) -> JSONResponse:
#     """
#     Arg: str representation of an int: 
#                                       get("/games/123456")
                                      
#     Returns: json info of the game
#     """
#     game = await read_game(data)
#     return game

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