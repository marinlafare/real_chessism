# database/operations/months.py

from fastapi.responses import PlainTextResponse
from database.database.db_interface import DBInterface
from database.database.models import Month
from database.operations.models import MonthCreateData, MonthResult
from sqlalchemy import select
from typing import List, Optional

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

