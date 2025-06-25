# database/database/ask_db.py (Fully Asynchronous and Batched Queries)

import asyncpg # Direct async PostgreSQL driver for low-level ops like DDL
from .db_interface import DBInterface # Assuming DBInterface is fully async
from .models import Player, Game, Month # Import SQLAlchemy ORM models (Player, Game, Month are expected)
from sqlalchemy import text, select # For raw SQL with SQLAlchemy async sessions, and ORM selects
from urllib.parse import urlparse # For parsing connection string for asyncpg direct connect
from typing import Tuple, Set, List, Dict, Any, Union

# --- Asynchronous Database Query Functions (using DBInterface/SQLAlchemy ORM) ---
async def get_async_db_session():
    """
    Provides an asynchronous database session using DBInterface.

    This function is the asynchronous SQLAlchemy equivalent to
    getting a database connection. It returns an async context manager
    for a database session.

    Usage:
        async with get_async_db_session() as session:
            # Perform database operations using 'session'
            # e.g., session.execute(select(MyModel))
    """
    # DBInterface can be initialized with any model; it's the session management
    # that's generic across models once the engine is set up.
    # We'll use the Game model as an example, but Player or any other works.
    session_interface = DBInterface(Game) # Using Game model as an example base for the engine
    return session_interface.AsyncSessionLocal()


async def get_players_already_in_db(player_names: Tuple[str, ...]) -> Set[str]:
    """
    Asynchronously checks and returns a set of player names already in the DB.
    Uses DBInterface for Player model. Handles large numbers of player names by batching queries.

    Args:
        player_names (Tuple[str, ...]): A tuple of player usernames to check.

    Returns:
        Set[str]: A set of player usernames that are already present in the database.
    """
    if not player_names:
        return set() # Return empty set if no player names are provided

    player_interface = DBInterface(Player) # Instantiate DBInterface for the Player model
    already_in_db_players = set()

    # Define a reasonable batch size for player names
    # Using 10000 for consistency and safety, similar to game links
    BATCH_SIZE = 10000 

    # Process player names in batches
    for i in range(0, len(player_names), BATCH_SIZE):
        batch_player_names = player_names[i:i + BATCH_SIZE]
        
        async with player_interface.AsyncSessionLocal() as session:
            # Using SQLAlchemy ORM filter for safety and consistency
            stmt = select(player_interface.model.player_name).filter(player_interface.model.player_name.in_(batch_player_names))
            result = await session.execute(stmt)
            already_in_db_players.update(result.scalars().all())

    return already_in_db_players


async def get_games_already_in_db(links: Tuple[int, ...]) -> Set[int]:
    """
    Asynchronously checks and returns a set of game links (IDs) already in the DB.
    Uses DBInterface for Game model. Handles large numbers of links by batching queries.

    Args:
        links (Tuple[int, ...]): A tuple of game links (IDs) to check.

    Returns:
        Set[int]: A set of game links (IDs) that are already present in the database.
    """
    if not links:
        return set() # Return empty set if no links are provided

    game_interface = DBInterface(Game) # Instantiate DBInterface for the Game model
    already_in_db_links = set()

    # Define a reasonable batch size to avoid the argument limit
    # The asyncpg limit is 32767, so a safe batch size is typically 10000 to be very safe.
    BATCH_SIZE = 10000

    # Process links in batches
    # This loop is crucial for handling the argument limit
    for i in range(0, len(links), BATCH_SIZE):
        batch_links = links[i:i + BATCH_SIZE]
        
        async with game_interface.AsyncSessionLocal() as session:
            # Using SQLAlchemy ORM filter for safety and consistency
            # This 'stmt' is executed for each small batch
            stmt = select(game_interface.model.link).filter(game_interface.model.link.in_(batch_links))
            result = await session.execute(stmt)
            already_in_db_links.update(result.scalars().all())

    return already_in_db_links


async def get_all_players() -> Set[str]:
    """
    Asynchronously fetches all player names from the database.
    Uses DBInterface for Player model.
    """
    player_interface = DBInterface(Player)
    async with player_interface.AsyncSessionLocal() as session:
        stmt = select(player_interface.model.player_name) # Select just the player_name column
        result = await session.execute(stmt)
        return set(result.scalars().all())

# --- Asynchronous Database Schema Management Functions (using asyncpg directly) ---

async def delete_all_tables(connection_string: str):
    """
    Asynchronously drops all tables in the 'public' schema of the database.
    This uses asyncpg directly for DDL operations, as it's often simpler
    for schema manipulation than going through the ORM.
    """
    # Parse the connection string to get details for asyncpg
    parsed_url = urlparse(connection_string)
    db_user = parsed_url.username
    db_password = parsed_url.password
    db_host = parsed_url.hostname
    db_port = parsed_url.port
    db_name = parsed_url.path.lstrip('/')

    conn = None # Initialize to None for finally block
    try:
        # Connect to the target database directly using asyncpg
        conn = await asyncpg.connect(
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
            database=db_name # Connect to the actual database
        )
        
        # Get list of tables in the public schema
        tables = await conn.fetch(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_type = 'BASE TABLE';
            """
        )

        if not tables:
            print(f"No tables found in 'public' schema of database '{db_name}' to delete.")
            return

        for record in tables:
            table_name = record['table_name']
            print(f"Deleting table: {table_name}...")
            try:
                # Use double quotes for table name to handle case-sensitivity or special characters
                # CASCADE ensures dependent objects (like foreign keys) are also dropped
                await conn.execute(f'DROP TABLE IF EXISTS "{table_name}" CASCADE;')
                print(f"Successfully deleted table: {table_name}")
            except Exception as e:
                print(f"Error deleting table '{table_name}': {e}")
                # Log or re-raise as appropriate. A single table failure might not stop others.
        print("All specified tables processed for deletion.")

    except Exception as e:
        print(f"An error occurred during delete_all_tables: {e}")
        raise # Re-raise the exception for higher-level handling
    finally:
        if conn:
            await conn.close()
            print("Database connection closed for deletion operation.")


async def open_request(sql_question: str, params: Union[Tuple[Any, ...], Dict[str, Any], None] = None, fetch_as_dict: bool = False) -> Union[List[Dict[str, Any]], List[Tuple[Any, ...]]]:
    """
    Executes a SQL query asynchronously using SQLAlchemy's AsyncSession.

    Args:
        sql_question (str): The SQL query string. Can be a raw SQL string.
        params (Union[Tuple[Any, ...], Dict[str, Any], None]): Parameters for the query.
            Can be a tuple for positional parameters or a dict for named parameters.
        fetch_as_dict (bool): If True, returns results as a list of dictionaries.
            Otherwise, returns as a list of tuples (default behavior of fetchall).

    Returns:
        Union[List[Dict[str, Any]], List[Tuple[Any, ...]]]: The fetched results.
    """
    # Use async with to ensure the session is properly opened and closed
    async with await get_async_db_session() as session:
        try:
            # For raw SQL, use text() to wrap the string.
            # Pass parameters directly to execute.
            result = await session.execute(text(sql_question), params)

            if fetch_as_dict:
                # Get column names from the result's keys
                column_names = result.keys()
                # Convert each row to a dictionary
                results = [dict(zip(column_names, row)) for row in result.fetchall()]
                return results
            else:
                # Return results as a list of tuples (default fetchall behavior)
                return result.fetchall()
        except Exception as e:
            # Log the error for debugging
            print(f"Error in open_request: {e}")
            # Depending on desired error handling, you might re-raise or return an empty list
            raise # Re-raise the exception to propagate it
