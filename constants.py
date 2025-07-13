# constants.py
import os
from dotenv import load_dotenv
from pathlib import Path

# Get the absolute path to the directory where this script resides
current_script_dir = Path(__file__).resolve().parent
env_file_name = '.env' # Changed back to '.env' as per your last message
env_file_absolute_path = current_script_dir / env_file_name

# Load environment variables from the specified .env file
load_dotenv(env_file_absolute_path, override=True)

# Retrieve environment variables.
USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")
HOST = os.getenv('HOST') # This will come from Docker Compose (host.docker.internal)
PORT = os.getenv("PORT") # This will come from Docker Compose (5432)
DATABASE_NAME = os.getenv("DATABASE_NAME")

# --- Validate and Convert PORT to Integer ---
if PORT is None:
    raise ValueError("PORT environment variable is not set. It must be provided by .env or Docker Compose.")
try:
    PORT_INT = int(PORT)
except ValueError:
    raise ValueError(f"PORT environment variable '{PORT}' cannot be converted to an integer.")

# --- Corrected Connection String Template for SQLAlchemy with asyncpg driver ---
# This uses 'postgresql+asyncpg://' to explicitly tell SQLAlchemy to use asyncpg.
CONN_STRING_TEMPLATE = "postgresql+asyncpg://{user}:{password}@{host}:{port}/{database_name}"

# Construct the full connection strings using the retrieved (and now correctly typed) values
CONN_STRING = CONN_STRING_TEMPLATE.replace('{user}', USER)
CONN_STRING = CONN_STRING.replace('{host}', HOST)
CONN_STRING = CONN_STRING.replace('{password}', PASSWORD)
CONN_STRING = CONN_STRING.replace('{port}', str(PORT_INT)) # Convert back to string for replacement
CONN_STRING = CONN_STRING.replace('{database_name}', DATABASE_NAME)

# Base connection string without the database name, for admin connections
BASE_DB_CONN_STRING = f"postgresql://{USER}:{PASSWORD}@{HOST}:{PORT_INT}"


# --- Debugging Start (to confirm loaded values) ---
print(f"DEBUG (constants.py): Loaded USER: '{USER}'")
print(f"DEBUG (constants.py): Loaded PASSWORD: '{'*' * len(PASSWORD) if PASSWORD else 'N/A'}'") # Mask password
print(f"DEBUG (constants.py): Loaded HOST: '{HOST}'")
print(f"DEBUG (constants.py): Loaded PORT: '{PORT}' (as string)")
print(f"DEBUG (constants.py): Parsed PORT_INT: '{PORT_INT}'")
print(f"DEBUG (constants.py): Loaded DATABASE_NAME: '{DATABASE_NAME}'")
print(f"DEBUG (constants.py): Constructed CONN_STRING: '{CONN_STRING}'") # This should now show postgresql+asyncpg
# --- Debugging End ---


# Other constants (no changes needed for these)
DRAW_RESULTS = ['50move', 'agreed', 'insufficient', 'repetition', 'stalemate', 'timevsinsufficient']
LOSE_RESULTS = ['checkmated', 'resigned', 'threecheck', 'timeout', 'abandoned']
WINING_RESULT = ['win', 'kingofthehill']
USER_AGENT = "ChessismApp/1.0 (marinlafare@gmail.com)"

# # constants.py
# import os
# from dotenv import load_dotenv
# load_dotenv('this_is_not_an_env.env')

# USER=os.getenv("USER")
# PASSWORD=os.getenv("PASSWORD")
# HOST=os.getenv('HOST')
# PORT=os.getenv("PORT")
# DATABASE_NAME=os.getenv("DATABASE_NAME")

# # --- FIX: Change the connection string dialect to use asyncpg ---
# # Original: postgresql://{user}:{password}@{host}:{port}/{database_name}
# # New:      postgresql+asyncpg://{user}:{password}@{host}:{port}/{database_name}

# CONN_STRING_TEMPLATE = "postgresql+asyncpg://{user}:{password}@{host}:{port}/{database_name}"


# CONN_STRING = CONN_STRING_TEMPLATE.replace('{user}',USER)
# CONN_STRING = CONN_STRING.replace('{host}',HOST)
# CONN_STRING = CONN_STRING.replace('{password}',PASSWORD)
# CONN_STRING = CONN_STRING.replace('{port}',PORT)
# CONN_STRING = CONN_STRING.replace('{database_name}',DATABASE_NAME)


# DRAW_RESULTS = ['50move', 'agreed', 'insufficient', 'repetition', 'stalemate', 'timevsinsufficient']
# LOSE_RESULTS = ['checkmated', 'resigned', 'threecheck', 'timeout', 'abandoned']
# WINING_RESULT = ['win', 'kingofthehill']
# USER_AGENT = "ChessismApp/1.0 (marinlafare@gmail.com)"