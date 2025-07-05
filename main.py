import os
from dotenv import load_dotenv
from fastapi import FastAPI
from contextlib import asynccontextmanager
from constants import CONN_STRING
from database.database.engine import init_db
from database.routers import games, players
from database.database.db_interface import DBInterface

# lifespan event handler for new implementation
@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db(CONN_STRING)
    DBInterface.initialize_engine_and_session(CONN_STRING)
    print('BASAL Server ON YO!...')
    yield
    print('BASAL Server DOWN YO!...')

app = FastAPI(lifespan=lifespan)

@app.get("/")
def read_root():
    return "MAIN_CHESSISM server running."

app.include_router(players.router)
app.include_router(games.router)