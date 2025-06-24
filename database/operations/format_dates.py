# database/operations/format_dates.py

from datetime import datetime
import calendar
from typing import Dict, Any, List, Tuple, Optional, Union

# Import necessary models and operations
from database.operations.models import MonthCreateData # Changed from MonthData based on recent models
from database.operations.months import create_month, read_months, update_month # Corrected import names
import database.operations.players as players_ops
from fastapi.encoders import jsonable_encoder
from database.database.db_interface import DBInterface
from database.database.models import Player, Month # SQLAlchemy ORM models
from sqlalchemy import select # Import select function for queries


async def get_joined_and_current_date(player_name: str) -> Dict[str, Any]:
    """
    Fetches player profile (inserting if new) and extracts joined and current dates.
    Returns a dictionary with 'joined_date' and 'current_date' or an 'error' key.
    """
    player_profile_result = await players_ops.insert_player({"player_name": player_name})

    if player_profile_result is None:
        print(f"Error: Could not retrieve or insert player profile for {player_name}.")
        return {"error": f"Failed to get player profile for {player_name}"}

    # player_profile_result is now guaranteed to be a PlayerCreateData instance
    joined_ts = player_profile_result.joined # Access the 'joined' attribute directly

    current_date = datetime.now()

    if joined_ts is None or joined_ts == 0: # Handle both None and the default 0 timestamp
        return {"error": "Joined date not found or is zero in player profile."}

    try:
        # Chess.com joined timestamp is in milliseconds, convert to seconds for fromtimestamp
        joined_date = datetime.fromtimestamp(joined_ts / 1000)
    except (TypeError, ValueError) as e:
        print(f"Error converting joined timestamp {joined_ts} for {player_name}: {e}")
        return {"error": f"Invalid joined date format for {player_name}"}
        
    return {"joined_date": joined_date, "current_date": current_date}


async def full_range(player_name: str) -> Union[List[str], Dict[str, Any]]:
    """
    Generates a list of 'YYYY-MM' month strings from player's joined date to current date.
    Returns a list of strings or a dictionary with an 'error' key.
    """
    dates_info = await get_joined_and_current_date(player_name)

    if "error" in dates_info:
        return dates_info # Propagate the error dictionary

    joined_date = dates_info["joined_date"]
    current_date = dates_info["current_date"]

    all_months = []
    # Start iterating from the beginning of the joined month
    current_month_iter = joined_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    # Iterate until the current month (inclusive)
    while current_month_iter <= current_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0):
        month_str = current_month_iter.strftime('%Y-%m')
        all_months.append(month_str)
        
        # Move to the next month
        # This correctly handles year rollover
        if current_month_iter.month == 12:
            current_month_iter = current_month_iter.replace(year=current_month_iter.year + 1, month=1)
        else:
            current_month_iter = current_month_iter.replace(month=current_month_iter.month + 1)
            
    return all_months


async def just_new_months(player_name: str) -> Union[List[str], Dict[str, Any], bool]:
    """
    Creates a list of tuples for every month the user has been in chess_com,
    months = [(2020,1),(2020,2),(2020,n)]
    filters the months already at the DB.
    
    Arg: player_name = "some_chess_com_user".lower()
    
    Returns on succes: a list of 'YYYY-MM' strings,
                otherwise an error dict, or False if no new months.
    """
    player_db_interface = DBInterface(Player)
    month_db_interface = DBInterface(Month)
    
    all_possible_months_strs = await full_range(player_name) # This returns List[str] or Dict[str,Any]
    
    if isinstance(all_possible_months_strs, dict) and "error" in all_possible_months_strs:
        return all_possible_months_strs # Propagate error from full_range

    existing_months_for_player = []
    async with month_db_interface.AsyncSessionLocal() as session:
        # Query Month records for the player
        if not hasattr(month_db_interface.Model, 'player_name') or \
           not hasattr(month_db_interface.Model, 'year') or \
           not hasattr(month_db_interface.Model, 'month'):
            print("Error: Month model missing expected attributes for querying existing months.")
            return {"error": "Month model definition issue."} # Or raise specific error

        # --- FIX: Correct SQLAlchemy select syntax ---
        stmt = select(month_db_interface.Model).filter_by(player_name=player_name)
        result = await session.execute(stmt)
        # Construct 'YYYY-MM' key for comparison
        existing_months_for_player = [f"{m.year}-{m.month:02d}" for m in result.scalars().all()]
    
    new_months_to_fetch = [
        month_str for month_str in all_possible_months_strs
        if month_str not in existing_months_for_player
    ]

    if not new_months_to_fetch:
        return False # No new months to fetch
    
    return new_months_to_fetch








# # database/operations/format_dates.py (REVISED - Focus on get_joined_and_current_date)

# from datetime import datetime
# import calendar
# from typing import Dict, Any, List, Tuple, Optional, Union # Import necessary types

# from database.operations.models import MonthCreateData # Changed from MonthData based on recent models
# from database.operations.months import create_month, read_months, update_month # Corrected import names
# import database.operations.players as players_ops
# from fastapi.encoders import jsonable_encoder
# from database.database.db_interface import DBInterface
# from database.database.models import Player, Month # SQLAlchemy ORM models

# async def get_joined_and_current_date(player_name: str) -> Dict[str, Any]:
#     """
#     Fetches player profile (inserting if new) and extracts joined and current dates.
#     Returns a dictionary with 'joined_date' and 'current_date' or an 'error' key.
#     """
#     # --- CRITICAL FIX: Ensure players_ops.insert_player is AWAITED and its return is handled ---
#     player_profile_result = await players_ops.insert_player({"player_name": player_name})

#     if player_profile_result is None:
#         # insert_player returns None if it fails to fetch or insert the profile
#         print(f"Error: Could not retrieve or insert player profile for {player_name}.")
#         return {"error": f"Failed to get player profile for {player_name}"}

#     # player_profile_result is now guaranteed to be a PlayerCreateData instance
#     # if it's not None.
    
#     # Extract joined_ts from the PlayerCreateData instance
#     # Pydantic models use direct attribute access
#     joined_ts = player_profile_result.joined # Access the 'joined' attribute directly

#     current_date = datetime.now()

#     if joined_ts is None or joined_ts == 0: # Handle both None and the default 0 timestamp
#         return {"error": "Joined date not found or is zero in player profile."}

#     try:
#         # Chess.com joined timestamp is in milliseconds, convert to seconds for fromtimestamp
#         joined_date = datetime.fromtimestamp(joined_ts / 1000)
#     except (TypeError, ValueError) as e:
#         print(f"Error converting joined timestamp {joined_ts} for {player_name}: {e}")
#         return {"error": f"Invalid joined date format for {player_name}"}
        
#     return {"joined_date": joined_date, "current_date": current_date}


# async def full_range(player_name: str) -> Union[List[str], Dict[str, Any]]:
#     """
#     Generates a list of 'YYYY-MM' month strings from player's joined date to current date.
#     Returns a list of strings or a dictionary with an 'error' key.
#     """
#     dates_info = await get_joined_and_current_date(player_name)

#     if "error" in dates_info:
#         return dates_info # Propagate the error dictionary

#     joined_date = dates_info["joined_date"]
#     current_date = dates_info["current_date"]

#     all_months = []
#     # Start iterating from the beginning of the joined month
#     current_month_iter = joined_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

#     # Iterate until the current month (inclusive)
#     while current_month_iter <= current_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0):
#         month_str = current_month_iter.strftime('%Y-%m')
#         all_months.append(month_str)
        
#         # Move to the next month
#         # This correctly handles year rollover
#         if current_month_iter.month == 12:
#             current_month_iter = current_month_iter.replace(year=current_month_iter.year + 1, month=1)
#         else:
#             current_month_iter = current_month_iter.replace(month=current_month_iter.month + 1)
            
#     return all_months


# async def just_new_months(player_name: str) -> Union[List[str], Dict[str, Any], bool]:
#     """
#     Identifies months for which game data needs to be downloaded (not already in DB).
#     Returns a list of 'YYYY-MM' strings, an error dict, or False if no new months.
#     """
#     player_db_interface = DBInterface(Player)
#     month_db_interface = DBInterface(Month) # Use DBInterface for Month model
    
#     all_possible_months_strs = await full_range(player_name) # This returns List[str] or Dict[str,Any]
    
#     if isinstance(all_possible_months_strs, dict) and "error" in all_possible_months_strs:
#         return all_possible_months_strs # Propagate error from full_range

#     existing_months_for_player = []
#     async with month_db_interface.AsyncSessionLocal() as session:
#         # Query Month records for the player
#         # Assuming Month model has 'player_name', 'year', 'month' columns
#         if not hasattr(month_db_interface.Model, 'player_name') or \
#            not hasattr(month_db_interface.Model, 'year') or \
#            not hasattr(month_db_interface.Model, 'month'):
#             print("Error: Month model missing expected attributes for querying existing months.")
#             return {"error": "Month model definition issue."} # Or raise specific error

#         stmt = month_db_interface.Model.select().filter_by(player_name=player_name)
#         result = await session.execute(stmt)
#         # Construct 'YYYY-MM' key for comparison
#         existing_months_for_player = [f"{m.year}-{m.month:02d}" for m in result.scalars().all()]
    
#     new_months_to_fetch = [
#         month_str for month_str in all_possible_months_strs
#         if month_str not in existing_months_for_player
#     ]

#     if not new_months_to_fetch:
#         return False # No new months to fetch
    
#     return new_months_to_fetch




# # database/operations/format_dates.py (FINAL REVISION for get_joined_and_current_date)

# from datetime import datetime
# import calendar
# from database.operations.models import MonthCreateData
# from database.operations.months import create_month, read_months, update_month # Use the actual function names
# import database.operations.players as players_ops
# from fastapi.encoders import jsonable_encoder
# from database.database.db_interface import DBInterface
# from database.database.models import Player, Month

# async def get_joined_and_current_date(player_name: str):
#     player_profile = await players_ops.insert_player({"player_name": player_name})

#     # --- FIX: Check the return value of insert_player more carefully ---
#     # insert_player can return a string error message or a player object (dict/Pydantic model)

#     if isinstance(player_profile, str) and ("Failed to fetch profile" in player_profile or "Invalid player data" in player_profile):
#         return {"error": player_profile} # Return error if player_profile is an error string

#     # Ensure player_profile is a dictionary if it's a Pydantic model or SQLAlchemy ORM object
#     player_profile_dict = {}
#     if hasattr(player_profile, 'model_dump'): # For Pydantic v2
#         player_profile_dict = player_profile.model_dump()
#     elif hasattr(player_profile, 'dict'): # For Pydantic v1
#         player_profile_dict = player_profile.dict()
#     elif isinstance(player_profile, dict):
#         player_profile_dict = player_profile
#     else:
#         # If player_profile is a SQLAlchemy ORM object directly, you might need to convert it
#         # For example, by converting to a dictionary of its columns/attributes
#         # This is a common pattern: `sqlalchemy.inspect(player_profile)._state.committed_state`
#         # or just access attributes directly if you know their names.
#         # For now, let's assume it has attributes like 'joined_date'
#         print(f"Warning: Player profile is not a dict or Pydantic model. Type: {type(player_profile)}")
#         # If it's a SQLAlchemy model, direct attribute access might work
#         player_profile_dict['joined_date'] = getattr(player_profile, 'joined_date', None)
#         # You might need to add other attributes if they are accessed later
#         # e.g., player_profile_dict['player_name'] = getattr(player_profile, 'player_name', None)
#         # For safety, ensure that the player_profile has 'joined_date' as an attribute
#         if not hasattr(player_profile, 'joined_date'):
#             return {"error": "Player profile object missing 'joined_date' attribute."}


#     joined_date_str = player_profile_dict.get('joined_date') # Use .get to avoid KeyError
#     current_date = datetime.now()

#     if joined_date_str is None: # Changed from not joined_date_str for clarity with None
#         return {"error": "Joined date not found in player profile"}

#     joined_date = datetime.fromtimestamp(joined_date_str / 1000) # Convert ms to date
#     return {"joined_date": joined_date, "current_date": current_date}


# async def full_range(player_name: str):
#     dates = await get_joined_and_current_date(player_name) # This is already awaited

#     if "error" in dates: # This check is now robust as 'dates' will be a dict or a string
#         return dates # Propagate the error

#     joined_date = dates["joined_date"]
#     current_date = dates["current_date"]

#     all_months = []
#     current_month_iter = joined_date.replace(day=1)

#     while current_month_iter <= current_date.replace(day=1):
#         month_str = current_month_iter.strftime('%Y-%m')
#         all_months.append(month_str)

#         # Move to the next month
#         if current_month_iter.month == 12:
#             current_month_iter = current_month_iter.replace(year=current_month_iter.year + 1, month=1)
#         else:
#             current_month_iter = current_month_iter.replace(month=current_month_iter.month + 1)

#     return all_months


# async def just_new_months(player_name: str):
#     player_db_interface = DBInterface(Player)
#     month_db_interface = DBInterface(Month)

#     all_possible_months = await full_range(player_name)

#     if "error" in all_possible_months:
#         return all_possible_months

#     existing_months_for_player = []
#     async with month_db_interface.get_session() as session:
#         result = await session.execute(
#             month_db_interface.Model.select().filter_by(player_name=player_name)
#         )
#         existing_months_for_player = [m.month_key for m in result.scalars().all()]

#     new_months_to_fetch = [
#         month for month in all_possible_months
#         if month not in existing_months_for_player
#     ]
#     return new_months_to_fetch










# # # operations/format_dates.py (REVISED)

# # import pandas as pd
# # from datetime import datetime
# # # from database.operations.players import insert_player # Will import async version
# # import database.operations.players as players_ops # Import module to access async functions
# # from database.database.ask_db import open_request # Assuming open_request is async compatible for your SQL query

# # # Assuming 'ask_db.py' and 'open_request' can handle async operations.
# # # If open_request is synchronous, calling it from an async function is okay,
# # # but it will block the event loop. For truly async DB operations,
# # # you'd need SQLAlchemy's async support (e.g., asyncpg driver, async sessionmaker).
# # # For now, we'll assume it works or that the blocking is minimal.


# # async def get_joined_and_current_date(player_name: str): # Make it async
# #     # AWAIT insert_player as it's now async
# #     # Ensure insert_player returns a dict with 'joined' or handles errors
# #     player_profile = await players_ops.insert_player({"player_name": player_name})
    
# #     if "error" in player_profile: # Handle potential errors from insert_player
# #         raise ValueError(f"Failed to get player profile: {player_profile['error']}")

# #     join_at = player_profile['joined']
# #     join_at_year = datetime.fromtimestamp(join_at).year
# #     join_at_month = datetime.fromtimestamp(join_at).month
# #     current_date = datetime.now().year, datetime.now().month
# #     return join_at_year, join_at_month, current_date[0], current_date[1]

# # def create_date_range(dates) -> list[tuple]:
# #     from_year, from_month, to_year, to_month = dates
# #     date_range = (
# #         pd.date_range(
# #             f"{from_year}-{from_month}-01", f"{to_year}-{to_month}-01", freq="MS"
# #             )
# #         .strftime("%Y-%m")
# #         .tolist()
# #     )
# #     date_range = [(int(x.split("-")[0]), int(x.split("-")[1])) for x in date_range]
# #     return date_range

# # async def full_range(player_name: str) -> list: # Make it async
# #     dates = await get_joined_and_current_date(player_name) # AWAIT this
# #     range_dates = create_date_range(dates) # create_date_range is sync, so no await needed
# #     return range_dates

# # async def just_new_months(player_name: str) -> list[tuple] | bool: # Make it async
# #     # AWAIT full_range as it's now async
# #     all_possible_months = await full_range(player_name)

# #     if not all_possible_months:
# #         return False

# #     possible_years = [m[0] for m in all_possible_months]
# #     possible_months = [m[1] for m in all_possible_months]

# #     sql_query = """
# #     SELECT
# #         pm.year_val,
# #         pm.month_val
# #     FROM
# #         UNNEST(%s::integer[], %s::integer[]) AS pm(year_val, month_val)
# #     LEFT JOIN
# #         months AS rm
# #         ON rm.player_name = %s
# #         AND rm.year = pm.year_val
# #         AND rm.month = pm.month_val
# #     WHERE
# #         rm.player_name IS NULL;
# #     """
    
# #     # If open_request is blocking (synchronous), calling it like this is technically fine
# #     # from an async function, but it will block the event loop.
# #     # If open_request can be made async, then it should be awaited.
# #     result_tuples = open_request(sql_query, params=(possible_years, possible_months, player_name))

# #     new_months_list = [(row[0], row[1]) for row in result_tuples]

# #     if not new_months_list:
# #         return False
# #     else:
# #         return new_months_list




# # #OPERATIONS FORMAT_DATES

# # import pandas as pd
# # from datetime import datetime
# # from database.operations.players import insert_player
# # from database.database.ask_db import open_request

# # def get_joined_and_current_date(player_name):
# #     join_at = insert_player({"player_name":player_name})['joined']
# #     join_at_year = datetime.fromtimestamp(join_at).year
# #     join_at_month = datetime.fromtimestamp(join_at).month
# #     current_date = datetime.now().year, datetime.now().month
# #     return join_at_year, join_at_month, current_date[0], current_date[1]
# # def create_date_range(dates) -> list[tuple]:
# #     from_year, from_month, to_year, to_month = dates
# #     date_range = (
# #         pd.date_range(
# #             f"{from_year}-{from_month}-01", f"{to_year}-{to_month}-01", freq="MS"
# #             )
# #         .strftime("%Y-%m")
# #         .tolist()
# #     )
# #     date_range = [(int(x.split("-")[0]), int(x.split("-")[1])) for x in date_range]
# #     return date_range
# # def full_range(player_name: str) -> list:
# #     dates = get_joined_and_current_date(player_name)
# #     range_dates = create_date_range(dates)
# #     return range_dates
# # def just_new_months(player_name: str) -> list[tuple] | bool: # Adjusted return type hint
# #     """
# #     Identifies months for which game data has not yet been recorded for a player.

# #     Args:
# #         player_name (str): The name of the player.

# #     Returns:
# #         list[tuple]: A list of (year, month) tuples that are new and need to be downloaded,
# #                       or False if all months are already in the database.
# #     """
# #     all_possible_months = full_range(player_name) # This generates (year, month) tuples

# #     if not all_possible_months:
# #         return False # No months to check if full_range is empty

# #     # Convert to a list of (year, month) dictionaries for easier parameterization if needed,
# #     # or just keep as tuples if your open_request supports passing list of tuples directly for UNNEST
# #     # Let's assume open_request can handle a list of (year, month) tuples with UNNEST for simplicity.

# #     # Extract years and months into separate lists for UNNEST
# #     possible_years = [m[0] for m in all_possible_months]
# #     possible_months = [m[1] for m in all_possible_months]

# #     # Optimized SQL query: Use LEFT JOIN to find months that do NOT exist in the 'months' table
# #     # and use parameterized query to prevent SQL injection.
# #     # We're effectively doing a set difference in the DB.
# #     sql_query = """
# #     SELECT
# #         pm.year_val,
# #         pm.month_val
# #     FROM
# #         UNNEST(%s::integer[], %s::integer[]) AS pm(year_val, month_val)
# #     LEFT JOIN
# #         months AS rm
# #         ON rm.player_name = %s
# #         AND rm.year = pm.year_val
# #         AND rm.month = pm.month_val
# #     WHERE
# #         rm.player_name IS NULL;
# #     """
    
# #     # Execute the query with parameters
# #     # The first %s is for possible_years, second for possible_months, third for player_name
# #     result_tuples = open_request(sql_query, params=(possible_years, possible_months, player_name))

# #     # result_tuples will contain (year, month) for the *new* months
# #     new_months_list = [(row[0], row[1]) for row in result_tuples]

# #     if not new_months_list:
# #         return False
# #     else:
# #         return new_months_list # Return as a list of tuples as expected



        
# # def just_new_months(player_name):
# #     date_range = full_range(player_name)
# #     recorded_months = open_request(f"select * from months where player_name = '{player_name}'")
# #     recorded_months = [(x[2],x[3]) for x in recorded_months]
    
# #     data_range = set(date_range)
# #     recorded_months = set(recorded_months)
# #     valid = data_range - recorded_months
# #     if len(valid) == 0:
# #         return False
# #     else:
# #         return valid