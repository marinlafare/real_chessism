#OPERATIONS
from pydantic import BaseModel
from typing import Optional

class PlayerCreateData(BaseModel):
    player_name: str
    name: Optional[str] = None
    url: Optional[str] = None
    title: Optional[str] = None
    avatar: Optional[str] = None
    followers: Optional[int] = None
    country: Optional[str] = None
    location: Optional[str] = None
    joined: Optional[int] = None
    status: Optional[str] = None
    is_streamer: Optional[bool] = None
    twitch_url: Optional[str] = None
    verified: Optional[bool] = False
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
    id: int
    player_name: str
    year: int
    month: int
    n_games: int