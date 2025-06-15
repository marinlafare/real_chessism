import os
from dotenv import load_dotenv
from fastapi import FastAPI
from contextlib import asynccontextmanager # Import asynccontextmanager
from constants import CONN_STRING
# Load environment variables

# Assuming these imports are correct and accessible
from database.database.engine import init_db
from database.routers import games, players

# Define the lifespan event handler
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Handles startup and shutdown events for the FastAPI application.
    Initializes the database on startup.
    """
    print('Server starting up...')
    init_db(CONN_STRING)
    yield # Application is now ready to receive requests
    print('Server shutting down...')
    # You can add shutdown logic here if needed, e.g., closing database connections

app = FastAPI(lifespan=lifespan) # Pass the lifespan to the FastAPI app

@app.get("/")
def read_root():
    """
    Root endpoint for the API.
    """
    return "Download server running."

# Include routers for games and players
app.include_router(players.router)
app.include_router(games.router)