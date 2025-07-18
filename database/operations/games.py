#OPERATIONS

from .available_months import just_new_months
from .format_games import format_games, insert_games_months_moves_and_players
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from .chess_com_api import download_months
from .months import get_most_recent_month, generate_months_from_date_to_now
from database.database.ask_db import open_request
import time

async def read_game(data):
    params = {"link": int(data)}
    sql_query =  """SELECT * 
                    FROM game
                    WHERE link = :link;"""
    result = await open_request(sql_query, params,fetch_as_dict=True)
    return result


async def create_games(data: dict) -> str:
    player_name = data['player_name'].lower()
    start_create_games = time.time()
    start_new_months = time.time()
    new_months = await just_new_months(player_name)
    if new_months is False:
        print('#####')
        print("MONTHS found: 0", 'time elapsed: ',time.time()-start_new_months)
        return 'ALL MONTHS IN DB ALREADY'
    else:
        print('#####')
        print(f"MONTHS found: {len(new_months)}", 'time elapsed: ',time.time()-start_new_months)
    print('... Starting DOWNLOAD ...')
    downloaded_games_by_month = await download_months(player_name, new_months)    
    num_downloaded_games = sum(len(v) for y in downloaded_games_by_month.values()
                               for v in y.values()) if downloaded_games_by_month else 0
    
    print(f"Processed {len(new_months)} months. Downloaded games: {num_downloaded_games}")
    print('#####')
    print('#####')
    print('Start the formating of the games')
    start_format = time.time()
    # THIS IS THE NEXT PART OF THE PROCESS
    formatted_games_results = await format_games(downloaded_games_by_month, player_name)
    print(f'FORMAT of {len(formatted_games_results)} games in: {time.time()-start_format}')
    await insert_games_months_moves_and_players(formatted_games_results, player_name)
    end_create_games = time.time()
    print('Format done in: ',(end_create_games-start_create_games)/60)
    return f"DATA READY FOR {player_name}"

async def update_player_games(player_name):
    start_create_games = time.time()
    current_months = await open_request("""
                select * from months where player_name = :player_name     
                """,params = {"player_name":player_name}, fetch_as_dict=True)
    
    most_recent_month = get_most_recent_month(current_months)
    new_months_for_update = generate_months_from_date_to_now(most_recent_month)
    downloaded_games_by_month = await download_months(player_name, new_months_for_update)
    
    num_downloaded_games = sum(len(v) for y in downloaded_games_by_month.values()
                               for v in y.values()) if downloaded_games_by_month else 0
    
    print(f"Processed {len(new_months_for_update)} months. Downloaded games: {num_downloaded_games}")
    print('#####')
    print('#####')
    print('Start the formating of the games')
    
    start_format = time.time()
    # THIS IS THE NEXT PART OF THE PROCESS
    formatted_games_results = await format_games(downloaded_games_by_month, player_name)
    print(f'FORMAT of {len(formatted_games_results)} games in: {time.time()-start_format}')
    await insert_games_months_moves_and_players(formatted_games_results, player_name)
    end_create_games = time.time()
    print('Format done in: ',(end_create_games-start_create_games)/60)
    return f"DATA READY FOR {player_name}"
    