# database/operations/months.py

import datetime

from fastapi.responses import PlainTextResponse
from database.database.db_interface import DBInterface
from database.database.models import Month
from database.operations.models import MonthCreateData, MonthResult
from sqlalchemy import select
from typing import List, Optional

pipi = 'lksajdfhl'
def get_most_recent_month(db_months:list):

    most_recent_entry = None
    most_recent_date = None

    for entry in db_months:
        try:
            year = entry.get('year')
            month = entry.get('month')

            if isinstance(year, int) and isinstance(month, int) and 1 <= month <= 12:
                current_date = datetime.date(year, month, 1)

                if most_recent_date is None or current_date > most_recent_date:
                    most_recent_date = current_date
                    most_recent_entry = entry
            else:
                print(f"Warning: Entry {entry} is missing valid 'year' or 'month' keys. Skipping.")

        except Exception as e:
            print(f"Error processing entry {entry}: {e}. Skipping.")
            continue 

    if most_recent_entry is None:
        return {}

    return most_recent_entry
async def read_months(player_name: str) -> Optional[List[MonthResult]]:
    """
    Reads all month records for a given player from the database.
    
    Arg: player_name = "someuser_chesscom"
    
    Returns: list[MonthResult] on success, return None if fails miserably
    """
    month_interface = DBInterface(Month)
    
    async with month_interface.AsyncSessionLocal() as session:
        
        select_months = select(Month).filter(Month.player_name == player_name)
        result = await session.execute(select_months)
        
        months_orms = result.scalars().all()

        if not months_orms:
            return None

        return [MonthResult(**month_interface.to_dict(m)) for m in months_orms]


async def update_month(data: dict) -> Optional[MonthResult]:
    """
    Updates an existing month record in the database.
    """
    month_interface = DBInterface(Month)
    data['player_name'] = data['player_name'].lower()
    
    # Validate input data with Pydantic
    try:
        month_data = MonthCreateData(**data) # Using CreateData for input, as it contains all updateable fields
    except Exception as e:
        print(f"Pydantic validation error for month update: {e}")
        return None # Indicate failure

    async with month_interface.AsyncSessionLocal() as session:
        # Find the month to update by its unique identifiers (player_name, year, month)
        stmt = select(Month).filter_by(
            player_name=month_data.player_name,
            year=month_data.year,
            month=month_data.month
        )
        existing_month_result = await session.execute(stmt)
        month_to_update_orm = existing_month_result.scalars().first()

        if not month_to_update_orm:
            print(f"Month {month_data.year}-{month_data.month} for {month_data.player_name} not found for update.")
            return None # Month not found

        # Update fields (e.g., n_games)
        # Use .model_dump() to get a dictionary of updated fields
        update_values = month_data.model_dump(exclude_unset=True) # Only update fields that were explicitly set
        
        for key, value in update_values.items():
            # Apply updates only for columns that exist in the ORM object
            if hasattr(month_to_update_orm, key):
                setattr(month_to_update_orm, key, value)
        
        try:
            await session.commit()
            await session.refresh(month_to_update_orm) # Refresh to get the latest state including any DB defaults/triggers
            return MonthResult(**month_interface.to_dict(month_to_update_orm)) # Convert ORM object to Pydantic model
        except Exception as e:
            await session.rollback()
            print(f"Error updating month in DB: {e}")
            return None
    return None # Should not be reached

import datetime
def generate_months_from_date_to_now(start_date_dict: dict):
    """
    Generates a list of 'YYYY-M' string tuples starting from the date specified
    in the input dictionary up to the current month and year.

    Args:
        start_date_dict (Dict[str, int]): A dictionary expected to have 'year' (int)
                                          and 'month' (int) keys, representing the start date.

    Returns:
        List[str]: A list of 'YYYY-M' strings.
                   Returns an empty list if the input dictionary is invalid
                   or the start date is in the future.
    """
    start_year = start_date_dict.get('year')
    start_month = start_date_dict.get('month')

    # Validate input
    if not (isinstance(start_year, int) and isinstance(start_month, int) and 1 <= start_month <= 12):
        print(f"Error: Invalid start_date_dict. Expected 'year' and 'month' as integers (month 1-12). Got: {start_date_dict}")
        return []

    current_date = datetime.date.today()
    start_date = datetime.date(start_year, start_month, 1)

    if start_date > current_date:
        print(f"Warning: Start date {start_date} is in the future. Returning empty list.")
        return []

    month_list = []
    temp_date = start_date

    while temp_date <= current_date:
        # Changed format to 'YYYY-M' string
        month_list.append(f"{temp_date.year}-{temp_date.month}")

        # Move to the next month
        if temp_date.month == 12:
            temp_date = datetime.date(temp_date.year + 1, 1, 1)
        else:
            temp_date = datetime.date(temp_date.year, temp_date.month + 1, 1)

    return month_list