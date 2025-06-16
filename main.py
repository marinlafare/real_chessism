import os
from dotenv import load_dotenv
from fastapi import FastAPI
from contextlib import asynccontextmanager
from constants import CONN_STRING
from database.database.engine import init_db
from database.routers import games, players

# lifespan event handler for new implementation
@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db(CONN_STRING)
    print('Server ON YO!...')
    yield
    print('Server DOWN YO!...')

app = FastAPI(lifespan=lifespan)

@app.get("/")
def read_root():
    return "chessism server running."

app.include_router(players.router)
app.include_router(games.router)