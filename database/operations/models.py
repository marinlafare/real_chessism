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
    id: int
    start_time: str
    year: int
    month:int
    day:int
    white:str
    black:str
    white_elo:int
    black_elo:int
    white_result:float
    black_result:float
    time_control:str
    eco:str
    time_elapsed:str
    termination:str
    n_moves:int
class MoveCreateData(BaseModel):
    id: int
    moves: str
    white_moves:str
    black_moves:str
    white_reaction_times:str
    black_reaction_times:str
    white_time_left:str
    black_time_left:str
class MonthCreateData(BaseModel):
    player_name: str
    dates: str