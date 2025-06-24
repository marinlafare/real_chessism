#OPERATIONS
from pydantic import BaseModel


# class PlayerCreateData(BaseModel):
#     player_name: str
#     name: str = 'None'
#     url: str = 'None'
#     title: str = 'None'
#     avatar: str = 'None'
#     followers: int = 0
#     country: str = 'None'
#     location: str = 'None'
#     joined: int = 0
#     status: str = 'None'
#     is_streamer: bool = False
#     twitch_url: str = 'None'
#     verified: bool = False
#     league: str = 'None'

from typing import Optional # Import Optional

class PlayerCreateData(BaseModel):
    player_name: str
    name: Optional[str] = None # Can be str or None, defaults to None
    url: Optional[str] = None
    title: Optional[str] = None # FIX: Changed from str = 'None' to Optional[str] = None
    avatar: Optional[str] = None
    followers: int = 0 # This is fine, int and defaults to 0
    country: Optional[str] = None
    location: Optional[str] = None # FIX: Changed from str = 'None' to Optional[str] = None
    joined: int = 0
    status: Optional[str] = None
    is_streamer: bool = False
    twitch_url: Optional[str] = None # FIX: Changed from str = 'None' to Optional[str] = None
    verified: Optional[bool] = False # Changed to Optional[bool] for consistency
    league: Optional[str] = None

class PlayerResult(BaseModel):
    player_name: str

class GameCreateData(BaseModel):
    link: int
    year: int
    month:int
    day:int
    hour: int
    minute: int
    second: int
    white:str
    black:str
    white_elo:int
    black_elo:int
    white_result:float
    black_result:float
    white_str_result:str
    black_str_result:str
    time_control:str
    eco:str
    time_elapsed:int
    n_moves:int
class MoveCreateData(BaseModel):
    link: int
    n_move: int
    white_move:str
    black_move:str
    white_reaction_time:float
    black_reaction_time:float
    white_time_left:float
    black_time_left:float
class MonthCreateData(BaseModel):
    player_name: str
    year: int
    month: int
    n_games: int
class MonthResult(BaseModel):
    # This model represents how a Month record looks when read from the DB
    id: int # Assuming 'id' is present after creation
    player_name: str
    year: int
    month: int
    n_games: int