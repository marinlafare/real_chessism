#OPERATIONS
from pydantic import BaseModel


class PlayerCreateData(BaseModel):
    player_name: str
    name: str = 'None'
    url: str = 'None'
    title: str = 'None'
    avatar: str = 'None'
    followers: int = 0
    country: str = 'None'
    location: str = 'None'
    joined: int = 0
    status: str = 'None'
    is_streamer: bool = False
    twitch_url: str = 'None'
    verified: bool = False
    league: str = 'None'

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