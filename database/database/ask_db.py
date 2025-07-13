# database/database/ask_db.py

import asyncpg
from .db_interface import DBInterface
from .models import Player, Game, Month 
from sqlalchemy import text, select 
from urllib.parse import urlparse
from typing import Tuple, Set, List, Dict, Any, Union

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
    session_interface = DBInterface(Game)
    return session_interface.AsyncSessionLocal()


async def get_games_already_in_db(links: Tuple[int, ...]) -> Set[int]:
    """
    Checks and returns a set of game links already in the DB.
    Uses DBInterface for Game model. Handles large numbers of links by batching queries.

    Args:
        links (Tuple[int, ...]): A tuple of game links (IDs) to check.

    Returns:
        Set[int]: A set of game links (IDs) that are already present in the database.
    """
    if not links:
        return set()

    game_interface = DBInterface(Game)
    already_in_db_links = set()

    BATCH_SIZE = 10000

    # Process links in batches
    # This loop is crucial for handling the argument limit
    for i in range(0, len(links), BATCH_SIZE):
        batch_links = links[i:i + BATCH_SIZE]
        
        async with game_interface.AsyncSessionLocal() as session:
            # Using SQLAlchemy ORM filter for safety and consistency
            batch_of_links = select(game_interface.model.link).filter(game_interface.model.link.in_(batch_links))
            result = await session.execute(batch_of_links)
            already_in_db_links.update(result.scalars().all())

    return already_in_db_links


async def get_all_players() -> Set[str]:
    """
    Asynchronously fetches all player names from the database.
    Uses DBInterface for Player model.
    """
    player_interface = DBInterface(Player)
    async with player_interface.AsyncSessionLocal() as session:
        stmt = select(player_interface.model.player_name)
        result = await session.execute(stmt)
        return set(result.scalars().all())

async def delete_all_main_tables(connection_string: str):
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

    conn = None 
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
            if record not in ['fen','known_fens','rawfen','knownfens','fens_ready']:
                table_name = record['table_name']
                
                print(f"Deleting table: {table_name}...")
                try:
                    await conn.execute(f'DROP TABLE IF EXISTS "{table_name}" CASCADE;')
                    print(f"Successfully deleted table: {table_name}")
                except Exception as e:
                    print(f"Error deleting table '{table_name}': {e}")
        print("All specified tables processed for deletion.")

    except Exception as e:
        print(f"An error occurred during delete_all_tables: {e}")
        raise
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
    async with await get_async_db_session() as session:
        try:
            result = await session.execute(text(sql_question), params)

            if fetch_as_dict:
                # Get column names from the result's keys
                column_names = result.keys()
                # Convert each row to a dictionary
                results = [dict(zip(column_names, row)) for row in result.fetchall()]
                return results
            else:
                return result.fetchall()
        except Exception as e:
            # Log the error for debugging
            print(f"Error in open_request: {e}")
            raise # if error then return anxiety
async def get_principal_players():
    """
    Retrieves a list of all player names from the 'player' table where 'name' is not null.

    Returns:
        List[str]: A list of player names (strings). Returns an empty list if no players are found
                   or if an error occurs.
    """
    try:
        # Assuming your player table is named 'player' and the column is 'player_name'
        # and you want to filter by a 'name' column not being null.
        # Adjust table and column names if they are different in your schema.
        query = "SELECT player_name FROM player WHERE name IS NOT NULL;"
        # Fetch as list of tuples, then extract the first element (player_name) from each tuple
        player_names_tuples = await open_request(query, fetch_as_dict=False)

        if player_names_tuples:
            return [name[0] for name in player_names_tuples]
        else:
            return []
    except Exception as e:
        print(f"Error in get_principal_players: {e}")
        return []