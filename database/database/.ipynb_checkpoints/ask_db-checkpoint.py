import os
from dotenv import load_dotenv
import requests
import pandas as pd
import tempfile
import psycopg2
from itertools import chain
from constants import CONN_STRING, PORT
def get_ask_connection():    
    return psycopg2.connect(CONN_STRING, port = PORT)

def player_exists_at_db(player_name: str):
    conn = get_ask_connection()
    with conn.cursor() as curs:
        curs.execute(
            f"select player_name from player where player_name='{player_name}'"
        )
        result = curs.fetchall()
    if len(result) == 1:
        return True
    return False

def open_request(sql_question:str):
    conn = get_ask_connection()
    with conn.cursor() as curs:
        curs.execute(
            sql_question
        )
        result = curs.fetchall()
    return result

def ask_months_in(player_name: str) -> list[tuple:tuple]:
    """
    It ask the db for the already asked months at chess.com
    returns them as a list of tuples
    """
    conn = get_ask_connection()
    with conn.cursor() as curs:
        curs.execute(f"select dates from months where player_name='{player_name}'")
        result = curs.fetchall()
    join_result = list(chain.from_iterable(result))
    result_as_list_of_tuples = [
        (int(x.split("-")[0]), int(x.split("-")[1])) for x in join_result
    ]
    return result_as_list_of_tuples


def ask_links_with_this_players(player_name, tuple_of_players) -> set():
    """
    Ask db the game.link for every past game of the player with any new user,
    we need to know it's some game is already at the database or if some player is not
    """
    conn = get_ask_connection()
    with conn.cursor() as curs:
        curs.execute(
            f"select link from game where white='{player_name}' and black in {tuple_of_players}"
        )
        result = curs.fetchall()
        curs.execute(
            f"select link from game where black='{player_name}' and white in {tuple_of_players}"
        )
        result_2 = curs.fetchall()
    result = set(list(chain.from_iterable(result)))
    result_2 = set(list(chain.from_iterable(result_2)))
    result.update(result_2)
    return set([int(x) for x in result])


def get_all_players():
    conn = get_ask_connection()
    with conn.cursor() as curs:
        curs.execute(f"select player_name from player")
        result = curs.fetchall()
    result = set(list(chain.from_iterable(result)))
    return result
def delete_all_tables():
    conn = get_ask_connection()
    conn.autocommit = True
    with conn.cursor() as curs:
        curs.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_type = 'BASE TABLE';
            """
        )
        tables = curs.fetchall()
    if not tables:
        print("No tables found in 'public' schema to delete.")
        return
    for table_row in tables:
        conn = get_ask_connection()
        conn.autocommit = True
        table_name = table_row[0]
        print(f"Deleting table: {table_name}...")
        try:
            with conn.cursor() as curs:
                curs.execute(f"DROP TABLE IF EXISTS \"{table_name}\" CASCADE;")
                print(f"Successfully deleted table: {table_name}")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
        finally:
            if conn:
                conn.close()
                print("Database connection closed.")