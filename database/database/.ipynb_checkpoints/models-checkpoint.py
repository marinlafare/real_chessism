#DATABASE
from typing import Any
from sqlalchemy import Column, ForeignKey, Integer, String, Float, BigInteger
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.types import Boolean
Base = declarative_base()


def to_dict(obj: Base) -> dict[str, Any]:
    return {c.name: getattr(obj, c.name) for c in obj.__table__.columns}

class Player(Base):
    __tablename__ = "player"
    player_name = Column("player_name", String, primary_key=True, nullable=False, unique=True)
    name = Column('name', String, nullable=True)
    url = Column('url', String, nullable=True)
    title = Column('title', String, nullable=True)
    avatar = Column('avatar', String, nullable=True)
    followers = Column('followers', Integer,nullable=True)
    country = Column('country', String, nullable=True)
    location = Column('location', String, nullable=True)
    joined = Column('joined', Integer, nullable=True)
    status = Column('status', String, nullable=True)
    is_streamer = Column('is_streamer', Boolean, nullable=True)
    twitch_url = Column('twitch_url', String, nullable=True)
    verified = Column('verified', Boolean, nullable=True)
    league = Column('league', String, nullable=True)
class Game(Base):
    __tablename__ = 'game'
    link = Column('link',BigInteger, primary_key = True, unique = True)
    white = Column("white", String, ForeignKey("player.player_name"), nullable=False)
    black = Column("black", String, ForeignKey("player.player_name"), nullable=False)

    year = Column("year", Integer, nullable=False)
    month = Column("month", Integer, nullable=False)
    day = Column("day", Integer, nullable=False)
    hour = Column("hour", Integer, nullable=False)
    minute = Column("minute", Integer, nullable=False)
    second = Column("second", Integer, nullable=False)
       
    white_elo = Column("white_elo", Integer, nullable=False)
    black_elo = Column("black_elo", Integer, nullable=False)
    white_result = Column("white_result", Float, nullable=False)
    black_result = Column("black_result", Float, nullable=False)
    white_str_result = Column("white_str_result", String, nullable=False)
    black_str_result = Column("black_str_result", String, nullable=False)
    time_control = Column("time_control", String, nullable=False)
    eco = Column("eco", String, nullable=False)
    time_elapsed = Column("time_elapsed", Integer, nullable=False)
    n_moves = Column("n_moves", Integer, nullable=False)
    fens_done = Column('fens_done', Boolean, nullable = False)
    
    white_player = relationship(Player, foreign_keys=[white])
    black_player = relationship(Player, foreign_keys=[black])
class Month(Base):
    __tablename__ = "months"
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    player_name = Column("player_name",
                         String,
                         ForeignKey("player.player_name"),
                         primary_key = False,
                         unique=False,
                         nullable=False)
    year = Column("year", Integer, nullable=False, unique=False)
    month = Column("month", Integer, nullable=False, unique=False)
    n_games = Column("n_games",Integer, nullable=False, unique=False)
    player = relationship(Player, foreign_keys=[player_name])

class Move(Base):
    __tablename__ = "moves"
    id = Column(Integer, primary_key=True, autoincrement=True)
    link = Column("link",BigInteger,ForeignKey("game.link"),
                  nullable=False,unique=False)
    n_move = Column("n_move", Integer, nullable=False)
    white_move = Column("white_move", String, nullable=False)
    black_move = Column("black_move", String, nullable=False)
    white_reaction_time = Column("white_reaction_time", Float, nullable=False)
    black_reaction_time = Column("black_reaction_time", Float, nullable=False)
    white_time_left = Column("white_time_left", Float, nullable=False)
    black_time_left = Column("black_time_left", Float, nullable=False)
    game = relationship(Game, foreign_keys=[link])


# temp_player_names_table = Table(
#     "temp_player_names",
#     Column("player_name", String, primary_key=True), # Use a primary key for efficient lookups
#     # You can add this if you want to ensure the table is temporary and cleaned up automatically
#     # postgresql_temporary=True, # This hint is for some ORM extensions, but manual DROP is safer with asyncpg
# )