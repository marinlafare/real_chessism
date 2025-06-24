import os
from dotenv import load_dotenv
load_dotenv('this_is_not_an_env.env') # Ensure this path is correct

USER=os.getenv("USER")
PASSWORD=os.getenv("PASSWORD")
HOST=os.getenv('HOST')
PORT=os.getenv("PORT")
DATABASE_NAME=os.getenv("DATABASE_NAME")

# --- FIX: Change the connection string dialect to use asyncpg ---
# Original: postgresql://{user}:{password}@{host}:{port}/{database_name}
# New:      postgresql+asyncpg://{user}:{password}@{host}:{port}/{database_name}

# Base connection string template
CONN_STRING_TEMPLATE = "postgresql+asyncpg://{user}:{password}@{host}:{port}/{database_name}"

# Replace placeholders
CONN_STRING = CONN_STRING_TEMPLATE.replace('{user}',USER)
CONN_STRING = CONN_STRING.replace('{host}',HOST)
CONN_STRING = CONN_STRING.replace('{password}',PASSWORD)
CONN_STRING = CONN_STRING.replace('{port}',PORT)
CONN_STRING = CONN_STRING.replace('{database_name}',DATABASE_NAME)


DRAW_RESULTS = ['50move', 'agreed', 'insufficient', 'repetition', 'stalemate', 'timevsinsufficient']
LOSE_RESULTS = ['checkmated', 'resigned', 'threecheck', 'timeout', 'abandoned']
WINING_RESULT = ['win', 'kingofthehill']
USER_AGENT = "ChessismApp/1.0 (marinlafare@gmail.com)"