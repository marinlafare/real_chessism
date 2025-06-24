# database/operations/months.py

from fastapi.responses import PlainTextResponse
from database.database.db_interface import DBInterface
from database.database.models import Month # Import SQLAlchemy Month model
from database.operations.models import MonthCreateData, MonthResult # Import Pydantic models
from sqlalchemy import select # Import select for building queries
from typing import List, Optional # Import List and Optional

# Note: html_response is generally defined in a higher-level module like routers or main.py
# if it's a common response type for FastAPI. Leaving it here for now as per your snippet.
def html_response(content):
    return PlainTextResponse(content=content, status_code=200)

async def read_months(player_name: str) -> Optional[List[MonthResult]]:
    """
    Reads all month records for a given player from the database.
    """
    month_interface = DBInterface(Month) # Instantiate DBInterface with SQLAlchemy Month model
    
    async with month_interface.AsyncSessionLocal() as session:
        # Build a query to select all Month objects for the given player_name
        stmt = select(Month).filter(Month.player_name == player_name)
        result = await session.execute(stmt)
        
        # Fetch all ORM objects
        months_orms = result.scalars().all()

        if not months_orms:
            return None # No months found for this player

        # Convert ORM objects to Pydantic MonthResult models using to_dict for simplicity
        # Ensure that month_interface.to_dict correctly extracts all fields for MonthResult
        return [MonthResult(**month_interface.to_dict(m)) for m in months_orms]


async def create_month(data: dict) -> Optional[MonthResult]:
    """
    Creates a new month record in the database.
    Checks for existing month to prevent duplicates based on player_name, year, and month.
    """
    month_interface = DBInterface(Month)
    data['player_name'] = data['player_name'].lower()
    
    # Validate input data with Pydantic
    try:
        month_data = MonthCreateData(**data)
    except Exception as e:
        print(f"Pydantic validation error for month creation: {e}")
        return None # Indicate failure

    # Check if this specific month already exists for the player to prevent duplicates
    async with month_interface.AsyncSessionLocal() as session:
        existing_month_stmt = select(Month).filter_by(
            player_name=month_data.player_name,
            year=month_data.year,
            month=month_data.month
        )
        existing_month_result = await session.execute(existing_month_stmt)
        existing_month_orm = existing_month_result.scalars().first()

        if existing_month_orm:
            print(f"Month {month_data.year}-{month_data.month} for {month_data.player_name} already exists. Skipping creation.")
            # Return the existing month as a MonthResult
            return MonthResult(**month_interface.to_dict(existing_month_orm))

    # If not existing, create the month asynchronously
    try:
        month_orm = await month_interface.create(month_data.model_dump())
        if month_orm: # Ensure creation was successful and it's an ORM object
            return MonthResult(**month_interface.to_dict(month_orm)) # Convert ORM object to Pydantic model
    except Exception as e:
        print(f"Error creating month in DB: {e}")
        # Consider logging the full traceback here for more details
        return None
    return None # Should not be reached if creation is handled in try-except


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


# #OPERATIONS
# #from .utils.ask_db import player_exists_at_db
# #from .chess_com_api import get_profile
# from database.database.db_interface import DBInterface
# from database.operations.models import MonthCreateData, MonthResult
# from fastapi.responses import PlainTextResponse


# def html_response(content):
#     return PlainTextResponse(content=content, status_code=200)

# def read_months(player_name: str, month_interface: DataInterface) -> MonthResult:
#     months = month_interface.player_exists(player_name)
#     if not months:
#         return False
#     return MonthResult(**months)

# def create_month(
#     data: dict, month_interface: DataInterface) -> MonthResult:
#     data['player_name'] = data['player_name'].lower()
#     month = month_interface.create(MonthCreateData(**data).model_dump())
#     return MonthResult(**month)
    
# def update_month(
#     data: dict, month_interface: DataInterface) -> MonthResult:
#     data['player_name'] = data['player_name'].lower()
#     month = month_interface.create(MonthCreateData(**data).model_dump())
#     return MonthResult(**month)
    
