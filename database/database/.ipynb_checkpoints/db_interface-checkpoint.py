# database/database/db_interface.py

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.future import select
from typing import Type, Dict, Any, List, Optional
import os

Base = declarative_base()

class DBInterface:
    _engine = None
    AsyncSessionLocal = None

    @classmethod
    def initialize_engine_and_session(cls, database_url: str):
        """
        Initializes the SQLAlchemy async engine and sessionmaker for the application.
        This method should be called ONCE during application startup.
        """
        if not database_url:
            raise ValueError("DATABASE_URL is wrong or something.")
        
        if cls._engine is None: # Only initialize if not already done
            cls._engine = create_async_engine(database_url, echo=False)
            cls.AsyncSessionLocal = sessionmaker(
                cls._engine, expire_on_commit=False, class_=AsyncSession
            )
            print("DBInterface: SQLAlchemy engine and AsyncSessionLocal initialized.")
        else:
            print("DBInterface: Engine already initialized, skipping.")


    def __init__(self, model: Type[Base]):
        """
        Initializes DBInterface for a specific model.
        Assumes that the engine and sessionmaker have already been initialized
        via DBInterface.initialize_engine_and_session() at application startup.
        """
        self.model = model
        if self.__class__._engine is None or self.__class__.AsyncSessionLocal is None:
            raise RuntimeError(
                "Database engine and session not initialized. "
                "Call DBInterface.initialize_engine_and_session() during application startup (e.g., in init_db)."
            )
        
    async def create(self, data: Dict[str, Any]) -> Base:
        """
        Creates a new record in the database.
        """
        async with self.AsyncSessionLocal() as session:
            async with session.begin():
                item = self.model(**data)
                session.add(item)
            await session.refresh(item)
            return item

    # async def read_by_name(self, name: str) -> Optional[Base]:
    #     """
    #     Reads a record by its 'player_name'.
    #     """
    #     async with self.AsyncSessionLocal() as session:
    #         result = await session.execute(
    #             select(self.model).filter_by(player_name=name)
    #         )
    #         return result.scalars().first()

    async def read_by_link(self, link_id: int) -> Optional[Base]:
        """
        Reads a game by it's link.
        """
        async with self.AsyncSessionLocal() as session:
            result = await session.execute(
                select(self.model).filter_by(link=link_id)
            )
            return result.scalars().first()

    async def read_all(self) -> List[Base]:
        """
        Reads all records for the model.
        """
        async with self.AsyncSessionLocal() as session:
            result = await session.execute(select(self.model))
            return result.scalars().all()

    async def update_by_name(self, name: str, new_data: Dict[str, Any]) -> Optional[Base]:
        """
        Updates a player by the player_name.
        """
        async with self.AsyncSessionLocal() as session:
            async with session.begin():
                # Fetch the existing record
                result = await session.execute(
                    select(self.model).filter_by(player_name=name)
                )
                item = result.scalars().first()
                
                if item:
                    # Update fields from new_data
                    for key, value in new_data.items():
                        setattr(item, key, value)
                    await session.flush() # Flush to apply changes before refresh
                    await session.refresh(item) # Refresh to load updated data
                    return item
                return None # Item not found for update

    async def update_by_link(self, link_id: int, new_data: Dict[str, Any]) -> Optional[Base]:
        """
        Updates a record identified by 'link' with new data.
        """
        async with self.AsyncSessionLocal() as session:
            async with session.begin():
                result = await session.execute(
                    select(self.model).filter_by(link=link_id)
                )
                item = result.scalars().first()
                
                if item:
                    for key, value in new_data.items():
                        setattr(item, key, value)
                    await session.flush()
                    await session.refresh(item)
                    return item
                return None

    async def delete_by_name(self, name: str) -> bool:
        """
        Deletes a record by its 'player_name'.
        """
        async with self.AsyncSessionLocal() as session:
            async with session.begin():
                result = await session.execute(
                    select(self.model).filter_by(player_name=name)
                )
                item = result.scalars().first()
                if item:
                    await session.delete(item)
                    return True
                return False

    async def delete_by_link(self, link_id: int) -> bool:
        """
        Deletes a record by its 'link'.
        """
        async with self.AsyncSessionLocal() as session:
            async with session.begin():
                result = await session.execute(
                    select(self.model).filter_by(link=link_id)
                )
                item = result.scalars().first()
                if item:
                    await session.delete(item)
                    return True
                return False

    async def create_all(self, data_list: List[Dict[str, Any]]) -> List[Base]:
        """
        Performs a bulk insert of records.
        """
        if not data_list:
            print(f"Warning: create_all called with empty data list for {self.model.__name__}. No action taken.")
            return []
            
        async with self.AsyncSessionLocal() as session:
            async with session.begin():
                items = [self.model(**data) for data in data_list]
                session.add_all(items)
                # No refresh needed for bulk insert, as individual items won't be used immediately after.
                # If you need IDs/updated states, you'd fetch them individually or use a different strategy.
            print(f"Successfully performed bulk insert for {len(data_list)} items in {self.model.__name__}.")
            return items

    def to_dict(self, obj: Base) -> Dict[str, Any]:
        """
        Converts a SQLAlchemy ORM object to a dictionary.
        For simplicity, this version only converts direct attributes.
        """
        return {c.name: getattr(obj, c.name) for c in obj.__table__.columns}

