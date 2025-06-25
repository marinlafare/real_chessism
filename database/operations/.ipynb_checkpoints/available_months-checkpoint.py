# database/operations/available_months.py

from datetime import datetime
import calendar
from typing import Dict, Any, List, Tuple, Optional, Union

from database.operations.models import MonthCreateData
from database.operations.months import create_month, read_months, update_month
import database.operations.players as players_ops
from fastapi.encoders import jsonable_encoder
from database.database.db_interface import DBInterface
from database.database.models import Player, Month
from sqlalchemy import select


async def get_joined_and_current_date(player_name: str) -> Dict[str, Any]:
    """
    Fetches player profile (inserting if new) and extracts the date they joined.

    Arg: player_name = "some_chess_com_user"
    
    Returns a dictionary with 'joined_date' and 'current_date' or an 'error' key.
    """
    profile = await players_ops.insert_player({"player_name": player_name})

    joined_ts = profile.joined

    current_date = datetime.now()

    if joined_ts is None or joined_ts == 0:
        return {"error": "Joined date not found or is zero in player profile."}

    try:
        joined_date = datetime.fromtimestamp(joined_ts)
    except (TypeError, ValueError) as e:
        print(f"Error converting joined timestamp {joined_ts} for {player_name}: {e}")
        return {"error": f"Invalid joined date format for {player_name}"}
        
    return {"joined_date": joined_date, "current_date": current_date}


async def full_range(player_name: str) -> Union[List[str], Dict[str, Any]]:
    """
    Generates a list of 'YYYY-MM' month strings
    from player's joined date to current date.

    Arg: player_name = "some_chess_com_user"
    
    Returns a list of strings or a dictionary with an 'error' key.
    """
    dates_info = await get_joined_and_current_date(player_name)

    if "error" in dates_info:
        return dates_info # you should feel ashamed for this

    joined_date = dates_info["joined_date"]
    current_date = dates_info["current_date"]

    all_months = []
    current_month_iter = joined_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    while current_month_iter <= current_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0):
        month_str = current_month_iter.strftime('%Y-%m')
        all_months.append(month_str)
        
        if current_month_iter.month == 12:
            current_month_iter = current_month_iter.replace(year=current_month_iter.year + 1, month=1)
        else:
            current_month_iter = current_month_iter.replace(month=current_month_iter.month + 1)
            
    return all_months


async def just_new_months(player_name: str) -> Union[List[str], Dict[str, Any], bool]:
    """
    Creates a str for every month the user has been in chess_com,
    months = ["2020-01","2020-02","2020-n"]
    filters the months already at the DB.
    
    Arg: player_name = "some_chess_com_user".lower()
    
    Returns on success: a list of 'YYYY-MM' strings,
                otherwise an error dict, or False if no new months.
    """
    
    all_possible_months_strs = await full_range(player_name) # List[str] or Dict[str,Any]
    
    if isinstance(all_possible_months_strs, dict) and "error" in all_possible_months_strs:
        return all_possible_months_strs # Shame on you

    existing_months_for_player = []
    month_db_interface = DBInterface(Month)
    async with month_db_interface.AsyncSessionLocal() as session:
    
        if not hasattr(month_db_interface.model, 'player_name') or \
           not hasattr(month_db_interface.model, 'year') or \
           not hasattr(month_db_interface.model, 'month'):
            print("Error: Month model missing expected attributes for querying existing months.")
            return {"error": "Month model definition issue. Idk seriously?"}

        player_db_months = select(month_db_interface.model).filter_by(player_name=player_name)
        result = await session.execute(player_db_months)
        existing_months_for_player = [f"{m.year}-{m.month:02d}" for m in result.scalars().all()]
    
    new_months_to_fetch = [
        month_str for month_str in all_possible_months_strs
        if month_str not in existing_months_for_player
    ]

    if not new_months_to_fetch:
        return False
    
    return new_months_to_fetch
