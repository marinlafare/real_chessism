#OPERATIONS

from .format_dates import just_new_months
from .format_games import format_and_insert_games
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from .chess_com_api import download_months
import time

def read_game(data):
    player = data
    player = jsonable_encoder(player)
    return JSONResponse(content=player)
# {"player_name":"lafareto"}

async def create_games(data: dict) -> str:
    player_name = data['player_name'].lower()
    start_create_games = time.time()
    start_new_months = time.time()
    new_months = await just_new_months(player_name)
    if new_months is False:
        print("MONTHS found: 0", 'time elapsed: ',time.time()-start_new_months)
        return 'ALL MONTHS IN DB ALREADY'
    else:
        print(f"MONTHS found: {len(new_months)}", 'time elapsed: ',time.time()-start_new_months)
    print('... Starting DOWNLOAD ...')
    start_download = time.time()
    downloaded_games_by_month = await download_months(player_name, new_months)
    download_time = time.time() - start_download
    print(f"DOWNLOADED IN: {download_time:.2f} seconds")

    return downloaded_games_by_month
    # # Example adjustment for the final return (if you want to keep str):
    # num_downloaded_games = sum(len(v) for y in downloaded_games_by_month.values() for v in y.values()) if downloaded_games_by_month else 0
    # print(f"Processed {len(new_months)} months. Downloaded games: {num_downloaded_games}")

    # # THIS IS THE NEXT PART OF THE PROCESS
    # format_and_insert_games(downloaded_games_by_month, player_name)
    # end_create_games = time.time()
    # print('Format done in: ',(end_create_games-start_create_games)/60)
    # return f"DATA READY FOR {player_name}"