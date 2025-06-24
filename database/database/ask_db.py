# database/database/ask_db.py (Fully Asynchronous)

import asyncpg # Direct async PostgreSQL driver for low-level ops like DDL
from .db_interface import DBInterface # Assuming DBInterface is fully async
from .models import Player, Game, Month # Import SQLAlchemy ORM models (Player, Game, Month are expected)
from sqlalchemy import text, select # For raw SQL with SQLAlchemy async sessions, and ORM selects
from urllib.parse import urlparse # For parsing connection string for asyncpg direct connect
from typing import Tuple, Set, List, Dict, Any, Union

# --- Asynchronous Database Query Functions (using DBInterface/SQLAlchemy ORM) ---

async def get_players_already_in_db(player_names: Tuple[str, ...]) -> Set[str]:
    """
    Asynchronously checks and returns a set of player names already in the DB.
    Uses DBInterface for Player model.
    """
    if not player_names:
        return set() # Return empty set if no player names are provided

    player_interface = DBInterface(Player) # Instantiate DBInterface for the Player model
    async with player_interface.AsyncSessionLocal() as session:
        # Using SQLAlchemy ORM filter for safety and consistency
        # Assuming 'player_name' is a column on the Player model
        stmt = select(player_interface.Model.player_name).filter(player_interface.Model.player_name.in_(player_names))
        result = await session.execute(stmt)
        return set(result.scalars().all())

async def get_games_already_in_db(links: Tuple[int, ...]) -> Set[int]:
    """
    Asynchronously checks and returns a set of game links (IDs) already in the DB.
    Uses DBInterface for Game model.
    """
    if not links:
        return set() # Return empty set if no links are provided

    game_interface = DBInterface(Game) # Instantiate DBInterface for the Game model
    async with game_interface.AsyncSessionLocal() as session:
        # Using SQLAlchemy ORM filter for safety and consistency
        # Assuming 'link' is a column on the Game model
        stmt = select(game_interface.Model.link).filter(game_interface.Model.link.in_(links))
        result = await session.execute(stmt)
        return set(result.scalars().all())


async def get_all_players() -> Set[str]:
    """
    Asynchronously fetches all player names from the database.
    Uses DBInterface for Player model.
    """
    player_interface = DBInterface(Player)
    async with player_interface.AsyncSessionLocal() as session:
        stmt = select(player_interface.Model.player_name) # Select just the player_name column
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





async def open_request(sql_question: str, params: tuple = None, fetch_as_dict: bool = False):
    conn = get_ask_connection()
    try:
        with conn.cursor() as curs:
            if params:
                curs.execute(sql_question, params)
            else:
                curs.execute(sql_question)
            
            if fetch_as_dict:
                column_names = [desc[0] for desc in curs.description]
                results = []
                for row in curs.fetchall():
                    results.append(dict(zip(column_names, row)))
                return results
            else:
                return curs.fetchall()
    finally:
        conn.close()

# # Example database/database/ask_db.py (Conceptual Async Update)
# import asyncpg # Or whatever async DB driver
# from .db_interface import DBInterface # Assuming DBInterface is now async

# async def get_players_already_in_db(player_names: tuple) -> set:
#     # This function needs to be async and query your database asynchronously
#     # Example with asyncpg or async SQLAlchemy
#     async with DBInterface(Player).AsyncSessionLocal() as session: # Assuming Player model available
#         result = await session.execute(
#             text(f"SELECT player_name FROM players WHERE player_name IN :player_names"),
#             {"player_names": player_names}
#         )
#         return set(result.scalars().all())

# async def get_games_already_in_db(links: tuple) -> set:
#     async with DBInterface(Game).AsyncSessionLocal() as session: # Assuming Game model available
#         result = await session.execute(
#             text(f"SELECT link FROM games WHERE link IN :links"),
#             {"links": links}
#         )
#         return set(result.scalars().all())

# # open_request might be redundant or integrated into DBInterface session management
# # async def open_request(query: str, params: dict = None):
# #     # This would also need to be async
# #     pass



# #DATABASE
# import asyncpg # Or whatever async DB driver
# from .db_interface import DBInterface # Assuming DBInterface is now async
# import os
# from dotenv import load_dotenv
# import requests
# import pandas as pd
# import tempfile
# import psycopg2
# from itertools import chain
# from constants import CONN_STRING, PORT
# def get_ask_connection():    
#     return psycopg2.connect(CONN_STRING, port = PORT)

# def player_exists_at_db(player_name: str):
#     conn = get_ask_connection()
#     with conn.cursor() as curs:
#         curs.execute(
#             f"select player_name from player where player_name='{player_name}'"
#         )
#         result = curs.fetchall()
#     if len(result) == 1:
#         return True
#     return False

# async def open_request(sql_question: str, params: tuple = None, fetch_as_dict: bool = False):
#     conn = get_ask_connection()
#     try:
#         with conn.cursor() as curs:
#             if params:
#                 curs.execute(sql_question, params)
#             else:
#                 curs.execute(sql_question)
            
#             if fetch_as_dict:
#                 column_names = [desc[0] for desc in curs.description]
#                 results = []
#                 for row in curs.fetchall():
#                     results.append(dict(zip(column_names, row)))
#                 return results
#             else:
#                 return curs.fetchall()
#     finally:
#         conn.close()

# def ask_months_in(player_name: str) -> list[tuple:tuple]:
#     """
#     It ask the db for the already asked months at chess.com
#     returns them as a list of tuples
#     """
#     conn = get_ask_connection()
#     with conn.cursor() as curs:
#         curs.execute(f"select dates from months where player_name='{player_name}'")
#         result = curs.fetchall()
#     join_result = list(chain.from_iterable(result))
#     result_as_list_of_tuples = [
#         (int(x.split("-")[0]), int(x.split("-")[1])) for x in join_result
#     ]
#     return result_as_list_of_tuples


# def ask_links_with_this_players(player_name, tuple_of_players) -> set():
#     """
#     Ask db the game.link for every past game of the player with any new user,
#     we need to know it's some game is already at the database or if some player is not
#     """
#     conn = get_ask_connection()
#     with conn.cursor() as curs:
#         curs.execute(
#             f"select link from game where white='{player_name}' and black in {tuple_of_players}"
#         )
#         result = curs.fetchall()
#         curs.execute(
#             f"select link from game where black='{player_name}' and white in {tuple_of_players}"
#         )
#         result_2 = curs.fetchall()
#     result = set(list(chain.from_iterable(result)))
#     result_2 = set(list(chain.from_iterable(result_2)))
#     result.update(result_2)
#     return set([int(x) for x in result])


# async def get_players_already_in_db(player_names: tuple) -> set:
#     # This function needs to be async and query your database asynchronously
#     # Example with asyncpg or async SQLAlchemy
#     async with DBInterface(Player).AsyncSessionLocal() as session: # Assuming Player model available
#         result = await session.execute(
#             text(f"SELECT player_name FROM players WHERE player_name IN :player_names"),
#             {"player_names": player_names}
#         )
#         return set(result.scalars().all())
# # def get_players_already_in_db(tuple_of_players):
# #     conn = get_ask_connection()
# #     with conn.cursor() as curs:
# #         if not isinstance(tuple_of_players, tuple):
# #             tuple_of_players = (tuple_of_players,)
# #         placeholders = ', '.join(['%s'] * len(tuple_of_players))
# #         query = f"select player_name FROM player WHERE player_name in ({placeholders})"
# #         curs.execute(query, tuple_of_players)
# #         result = curs.fetchall()
# #     result = set(list(chain.from_iterable(result)))
# #     return result
# # def get_games_already_in_db(tuple_of_links):
    
# #     conn = get_ask_connection()
# #     with conn.cursor() as curs:
# #         if not isinstance(tuple_of_links, tuple):
# #             tuple_of_links = (tuple_of_links,)
# #         placeholders = ', '.join(['%s'] * len(tuple_of_links))
# #         query = f"select link FROM game WHERE link in ({placeholders})"
# #         curs.execute(query, tuple_of_links)
# #         result = curs.fetchall()
# #     result = set(list(chain.from_iterable(result)))
# #     return result
# async def get_games_already_in_db(links: tuple) -> set:
#     async with DBInterface(Game).AsyncSessionLocal() as session: # Assuming Game model available
#         result = await session.execute(
#             text(f"SELECT link FROM games WHERE link IN :links"),
#             {"links": links}
#         )
#         return set(result.scalars().all())



# def get_all_players():
#     conn = get_ask_connection()
#     with conn.cursor() as curs:
#         curs.execute(f"select player_name from player")
#         result = curs.fetchall()
#     result = set(list(chain.from_iterable(result)))
#     return result
# def delete_all_tables():
#     conn = get_ask_connection()
#     conn.autocommit = True
#     with conn.cursor() as curs:
#         curs.execute(
#             """
#             SELECT table_name
#             FROM information_schema.tables
#             WHERE table_schema = 'public'
#             AND table_type = 'BASE TABLE';
#             """
#         )
#         tables = curs.fetchall()
#     if not tables:
#         print("No tables found in 'public' schema to delete.")
#         return
#     for table_row in tables:
#         conn = get_ask_connection()
#         conn.autocommit = True
#         table_name = table_row[0]
#         print(f"Deleting table: {table_name}...")
#         try:
#             with conn.cursor() as curs:
#                 curs.execute(f"DROP TABLE IF EXISTS \"{table_name}\" CASCADE;")
#                 print(f"Successfully deleted table: {table_name}")
#         except Exception as e:
#             print(f"An unexpected error occurred: {e}")
#         finally:
#             if conn:
#                 conn.close()
#                 print("Database connection closed.")





# # def get_players_already_in_db___(tuple_of_players):
# #     conn = get_ask_connection()
# #     with conn.cursor() as curs:
# #         curs.execute(f"select player_name from player where player_name in {tuple_of_players}")
# #         result = curs.fetchall()
# #     result = set(list(chain.from_iterable(result)))
# #     return result
    