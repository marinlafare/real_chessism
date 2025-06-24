# database/database/db_interface.py

from typing import Any, List, Dict, Type, TypeVar, Optional
from sqlalchemy.orm import Session
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import exists, select # Import select for building queries
from sqlalchemy import text # For raw SQL if needed, but select is better

# Assuming these are defined in database/database/engine.py
# and that `Base` is also imported from `database.database.models` or defined here.
from database.database.engine import AsyncDBSession # Import the sessionmaker
from database.database.models import Base, to_dict # Import Base and to_dict from your models file

# Define type variables for clearer type hinting
DataObject = Dict[str, Any]
ListOfDataObjects = List[DataObject]
TBase = TypeVar('TBase', bound=Base)

class DBInterface:
    """
    A generic asynchronous database interface for SQLAlchemy models.
    """

    def __init__(self, db_class: Type[TBase]):
        """
        Initializes the DBInterface with a specific SQLAlchemy model class.

        Args:
            db_class (Type[Base]): The SQLAlchemy declarative base class (e.g., Player, Game, etc.).
        """
        self.db_class = db_class
        # Provide access to the globally configured AsyncDBSession sessionmaker
        self.AsyncSessionLocal = AsyncDBSession
        # Make to_dict available within the instance if it's a helper function
        self.to_dict = to_dict
        # Store the model class for direct use in queries (e.g., select(self.Model))
        self.Model = db_class

    async def read(self, primary_key_value: Any) -> Optional[DataObject]:
        """
        Reads a single record by its primary key asynchronously.
        """
        async with self.AsyncSessionLocal() as session:
            data: TBase | None = await session.get(self.db_class, primary_key_value)
            if data is None:
                return None
            return self.to_dict(data)

    async def player_exists(self, player_name: str) -> bool:
        """
        Checks asynchronously if a player exists by their player_name.
        (Assumes this DBInterface instance is for the Player model)
        """
        async with self.AsyncSessionLocal() as session:
            # Use SQLAlchemy Core/ORM constructs for existence check
            # Assumes 'player_name' column exists on self.db_class (e.g., Player model)
            if not hasattr(self.db_class, 'player_name'):
                raise AttributeError(f"Model {self.db_class.__name__} does not have 'player_name' attribute for player_exists check.")
            
            stmt = select(exists().where(self.db_class.player_name == player_name))
            result = await session.scalar(stmt)
            return bool(result)

    async def read_by_name(self, player_name: str) -> Optional[DataObject]:
        """
        Reads a single record by a 'name' field (e.g., player_name) asynchronously.
        (Assumes this DBInterface instance is for the Player model with player_name)
        """
        async with self.AsyncSessionLocal() as session:
            # Assumes 'player_name' column exists on self.db_class
            if not hasattr(self.db_class, 'player_name'):
                raise AttributeError(f"Model {self.db_class.__name__} does not have 'player_name' attribute for read_by_name.")
            
            stmt = select(self.db_class).filter(self.db_class.player_name == player_name)
            result = await session.execute(stmt)
            data: TBase | None = result.scalars().first()
            if data is None:
                return None
            return self.to_dict(data)

    async def create(self, data: DataObject) -> TBase:
        """
        Inserts a single record into the database asynchronously.
        Returns the SQLAlchemy ORM object after creation and refresh.
        """
        async with self.AsyncSessionLocal() as session:
            item: TBase = self.db_class(**data)
            session.add(item)
            await session.commit()
            await session.refresh(item)
            return item # Return the ORM object

    async def create_all(self, data_list: ListOfDataObjects) -> None:
        """
        Inserts multiple records into the database using SQLAlchemy's bulk_insert_mappings asynchronously.
        """
        if not data_list:
            print(f"Warning: create_all called with empty data list for {self.db_class.__tablename__}. No action taken.")
            return

        async with self.AsyncSessionLocal() as session:
            try:
                # bulk_insert_mappings is synchronous, so it needs to be run in a sync context within run_sync
                await session.run_sync(
                    lambda s: s.bulk_insert_mappings(self.db_class, data_list)
                )
                await session.commit()
            except Exception as e:
                await session.rollback()
                print(f"Error during bulk insert for {self.db_class.__tablename__}: {e}")
                raise

    async def update(self, primary_key_value: Any, data: DataObject) -> Optional[DataObject]:
        """
        Updates a single record by its primary key asynchronously.
        """
        async with self.AsyncSessionLocal() as session:
            item: TBase | None = await session.get(self.db_class, primary_key_value)
            if item is None:
                print(f"Record with PK {primary_key_value} not found for update in {self.db_class.__tablename__}.")
                return None
            for key, value in data.items():
                if hasattr(item, key):
                    setattr(item, key, value)
                else:
                    print(f"Warning: Attempted to set non-existent attribute '{key}' on {self.db_class.__tablename__} during update.")
            try:
                await session.commit()
                await session.refresh(item)
                return self.to_dict(item)
            except Exception as e:
                await session.rollback()
                print(f"Error updating item with PK {primary_key_value} for {self.db_class.__tablename__}: {e}")
                raise

    async def delete(self, primary_key_value: Any) -> Optional[DataObject]:
        """
        Deletes a single record by its primary key asynchronously.
        """
        async with self.AsyncSessionLocal() as session:
            item: TBase | None = await session.get(self.db_class, primary_key_value)
            if item is None:
                print(f"Record with PK {primary_key_value} not found for deletion in {self.db_class.__tablename__}.")
                return None
            result = self.to_dict(item)
            try:
                await session.delete(item)
                await session.commit()
                return result
            except Exception as e:
                await session.rollback()
                print(f"Error deleting item with PK {primary_key_value} for {self.db_class.__tablename__}: {e}")
                raise



# # database/database/db_interface.py (Conceptual Async Update)
# import asyncpg # Example for PostgreSQL, use appropriate driver for your DB
# from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession # If using SQLAlchemy 2.0
# from sqlalchemy.orm import sessionmaker
# from sqlalchemy import text # For raw SQL if needed
# from constants import CONN_STRING
# class DBInterface:
#     def __init__(self, model):
#         self.model = model
#         # Configure your async database connection here
#         # Example using asyncpg directly (simplified)
#         # self.conn_pool = None # Or manage connection pool globally
#         # Example using SQLAlchemy 2.0 async engine
#         self.engine = create_async_engine(CONN_STRING, echo=False) # CONN_STRING needs to be async-compatible
#         self.AsyncSessionLocal = sessionmaker(
#             self.engine, expire_on_commit=False, class_=AsyncSession
#         )

#     async def get_session(self):
#         async with self.AsyncSessionLocal() as session:
#             yield session

#     async def create(self, data: dict):
#         async with self.AsyncSessionLocal() as session:
#             db_obj = self.model(**data)
#             session.add(db_obj)
#             await session.commit()
#             await session.refresh(db_obj)
#             return db_obj

#     async def create_all(self, data_list: list[dict]):
#         async with self.AsyncSessionLocal() as session:
#             db_objs = [self.model(**data) for data in data_list]
#             session.add_all(db_objs)
#             await session.commit()
#             # No refresh for all, you'd iterate if needed for individual refreshed objects

#     async def player_exists(self, player_name: str) -> bool:
#         async with self.AsyncSessionLocal() as session:
#             result = await session.execute(
#                 text(f"SELECT EXISTS(SELECT 1 FROM players WHERE player_name = :player_name)"), # Adjust table name
#                 {"player_name": player_name}
#             )
#             return result.scalar_one()

#     async def read_by_name(self, player_name: str):
#         async with self.AsyncSessionLocal() as session:
#             result = await session.execute(
#                 text(f"SELECT * FROM players WHERE player_name = :player_name"),
#                 {"player_name": player_name}
#             )
#             return result.scalars().first() # Returns the SQLAlchemy model instance










# # database/database/db_interface.py

# from typing import Any, List, Dict, Type, TypeVar
# from sqlalchemy.orm import Session
# from sqlalchemy.ext.declarative import declarative_base # Assuming Base is defined here or imported from models
# from sqlalchemy.sql import exists
# from sqlalchemy import select

# # Assuming these are defined in database/database/engine.py
# # and that `Base` is also imported from `database.database.models` or defined here.
# # For this script, we'll assume `DBSession` and `Base` come from where they are intended.
# from database.database.engine import DBSession
# from database.database.models import Base, to_dict # Import Base and to_dict from your models file

# # Define type variables for clearer type hinting
# DataObject = Dict[str, Any]
# ListOfDataObjects = List[DataObject]
# TBase = TypeVar('TBase', bound=Base)

# class DBInterface:
#     """
#     A generic database interface for SQLAlchemy models, providing CRUD operations
#     with a focus on efficient bulk insertion.
#     """

#     def __init__(self, db_class: Type[TBase]):
#         """
#         Initializes the DBInterface with a specific SQLAlchemy model class.

#         Args:
#             db_class (Type[Base]): The SQLAlchemy declarative base class (e.g., Player, Game, etc.).
#         """
#         self.db_class = db_class

#     def read(self, primary_key_value: Any) -> DataObject | None:
#         """
#         Reads a single record by its primary key.
#         Assumes the table has a single-column primary key. For composite primary keys,
#         this method would need to be extended or a more specific query used.

#         Args:
#             primary_key_value (Any): The value of the primary key for the record to retrieve.

#         Returns:
#             DataObject | None: A dictionary representation of the record if found, otherwise None.
#         """
#         session: Session = DBSession()
#         try:
#             # session.get() is the modern and efficient way to fetch by primary key
#             data: TBase | None = session.get(self.db_class, primary_key_value)
#             if data is None:
#                 return None
#             return to_dict(data)
#         finally:
#             session.close()

#     def create(self, data: DataObject) -> DataObject:
#         """
#         Inserts a single record into the database.

#         Args:
#             data (DataObject): A dictionary representing the data for the new record.
#                                Keys should match the SQLAlchemy model's column names.

#         Returns:
#             DataObject: A dictionary representation of the newly created record,
#                         including any database-generated values (like auto-incremented IDs).
#         """
#         session: Session = DBSession()
#         try:
#             item: TBase = self.db_class(**data)
#             session.add(item)
#             session.commit()
#             session.refresh(item) # Refresh to ensure auto-incremented IDs or defaults are loaded
#             result = to_dict(item)
#             return result
#         except Exception as e:
#             session.rollback() # Rollback on error
#             print(f"Error creating single item for {self.db_class.__tablename__}: {e}")
#             raise # Re-raise the exception to propagate it
#         finally:
#             session.close()

#     def create_all(self, data: ListOfDataObjects) -> bool:
#         """
#         Inserts multiple records into the database using SQLAlchemy's bulk_insert_mappings.
#         This is efficient for inserting large lists of dictionaries.

#         Args:
#             data (ListOfDataObjects): A list of dictionaries, where each dictionary
#                                       represents a row to insert. Keys in the dictionaries
#                                       must match the SQLAlchemy model's column names.
#                                       Assumes data has been "lintered by pydantic modules"
#                                       meaning it's already in the correct dict format.

#         Returns:
#             bool: True if the bulk insertion was successful. Raises an exception on failure.
#         """
#         if not data: # Handle an empty list of data gracefully
#             print(f"Warning: create_all called with empty data list for {self.db_class.__tablename__}. No action taken.")
#             return True

#         session: Session = DBSession()
#         try:
#             # bulk_insert_mappings is highly efficient as it works directly with dictionaries
#             # and bypasses ORM object creation for each row, performing a single multi-value INSERT.
#             session.bulk_insert_mappings(self.db_class, data)
#             session.commit()
#             return True
#         except Exception as e:
#             session.rollback() # Ensure rollback on any error during the bulk operation
#             print(f"Error during bulk insert for {self.db_class.__tablename__}: {e}")
#             raise # Propagate the exception
#         finally:
#             session.close()

#     def update(self, primary_key_value: Any, data: DataObject) -> DataObject | None:
#         """
#         Updates a single record identified by its primary key with new data.

#         Args:
#             primary_key_value (Any): The primary key value of the record to update.
#             data (DataObject): A dictionary of column-value pairs to update.

#         Returns:
#             DataObject | None: A dictionary representation of the updated record if found,
#                                otherwise None.
#         """
#         session: Session = DBSession()
#         try:
#             item: TBase | None = session.get(self.db_class, primary_key_value)
#             if item is None:
#                 print(f"Record with PK {primary_key_value} not found for update in {self.db_class.__tablename__}.")
#                 return None
#             for key, value in data.items():
#                 # Only set attribute if it exists on the ORM object to prevent AttributeError
#                 if hasattr(item, key):
#                     setattr(item, key, value)
#                 else:
#                     print(f"Warning: Attempted to set non-existent attribute '{key}' on {self.db_class.__tablename__} during update.")
#             session.commit()
#             session.refresh(item) # Refresh to load any updated values from the DB
#             return to_dict(item)
#         except Exception as e:
#             session.rollback()
#             print(f"Error updating item with PK {primary_key_value} for {self.db_class.__tablename__}: {e}")
#             raise
#         finally:
#             session.close()

#     def delete(self, primary_key_value: Any) -> DataObject | None:
#         """
#         Deletes a single record identified by its primary key.

#         Args:
#             primary_key_value (Any): The primary key value of the record to delete.

#         Returns:
#             DataObject | None: A dictionary representation of the deleted record if successful,
#                                otherwise None if the record was not found.
#         """
#         session: Session = DBSession()
#         try:
#             item: TBase | None = session.get(self.db_class, primary_key_value)
#             if item is None:
#                 print(f"Record with PK {primary_key_value} not found for deletion in {self.db_class.__tablename__}.")
#                 return None
#             result = to_dict(item) # Get dict before deletion
#             session.delete(item)
#             session.commit()
#             return result
#         except Exception as e:
#             session.rollback()
#             print(f"Error deleting item with PK {primary_key_value} for {self.db_class.__tablename__}: {e}")
#             raise
#         finally:
#             session.close()



# # database/database/db_interface.py (Update)
# from typing import Any, List, Dict, TypeVar, Type
# from database.database.engine import DBSession # Assuming DBSession is correctly imported
# from database.database.models import Base # Assuming Base is defined here or imported
# from sqlalchemy.orm import Session # For type hinting

# # Define DataObject more explicitly for lists of data
# DataObject = Dict[str, Any]
# ListOfDataObjects = List[DataObject]

# # Define a TypeVar for Base subclasses to improve type hinting for db_class
# TBase = TypeVar('TBase', bound=Base)

# class DBInterface:

#     def __init__(self, db_class: Type[TBase]): # Use Type[TBase] for the db_class
#         self.db_class = db_class

#     # --- NEW GENERIC READ METHOD ---
#     def read(self, primary_key_value: Any) -> DataObject | None:
#         """
#         Reads a single record by its primary key.
#         Assumes the table has a single-column primary key that matches the value type.
#         """
#         session: Session = DBSession()
#         try:
#             # session.get is the modern way to fetch by primary key
#             data: TBase | None = session.get(self.db_class, primary_key_value)
#             if data is None:
#                 return None
#             return to_dict(data)
#         finally:
#             session.close()

#     # (Keep your existing read_fen, create, create_all, update, delete methods)
#     # If you intend to use read_fen only for FEN models and read for others, that's fine.
#     # Otherwise, you could remove read_fen and just use the generic read.
#     # For now, let's keep read_fen if it's used elsewhere for clarity,
#     # but the generic 'read' covers its functionality.
    
#     # Original read_fen (if you keep it):
#     def read_fen(self, fen: str) -> DataObject | None:
#         session: Session = DBSession()
#         try:
#             data: Base | None = session.get(self.db_class, fen)
#             if data is None:
#                 return None
#             return to_dict(data)
#         finally:
#             session.close()

#     def create(self, data: DataObject) -> DataObject:
#         # ... (your existing create method) ...
#         pass

#     def create_all(self, data: ListOfDataObjects) -> bool:
#         # ... (your existing create_all method) ...
#         pass

#     def update(self, primary_key_value: Any, data: DataObject) -> DataObject | None:
#         # ... (your existing update method) ...
#         pass

#     def delete(self, primary_key_value: Any) -> DataObject | None:
#         # ... (your existing delete method) ...
#         pass

# # Your to_dict function (assuming it's here or imported)
# def to_dict(obj: Base) -> dict[str, Any]:
#     return {c.name: getattr(obj, c.name) for c in obj.__table__.columns}

# # DATABASE
# from typing import Any

# from database.database.engine import DBSession
# from database.database.models import Base, to_dict
# from sqlalchemy import exists
# from sqlalchemy.orm import sessionmaker, Session
# from sqlalchemy import select

# DataObject = dict[str, Any]


# class DBInterface:
    
#     def __init__(self, db_class: type[Base]):
#         self.db_class = db_class

#     def player_exists(self, player_name: str) -> DataObject:
#         session = DBSession()
#         item: Base = session.query(exists().\
#         where(self.db_class.player_name==player_name)).scalar()
#         if str(item) == 'True':
#             return True
#         else:
#             return False
#     def read_by_name(self, player_name: str)->DataObject:
#         session = DBSession()
#         data: Base = session.query(self.db_class).get(player_name)
#         session.close()
#         if data == None:
#             return None
#         return to_dict(data)
#     def create(self, data: DataObject) -> DataObject:
#         session = DBSession()
#         item: Base = self.db_class(**data)
#         session.add(item)
#         session.commit()
#         result = to_dict(item)
#         session.close()
#         return result   
    
#     def create_all(self, data: DataObject) -> DataObject:
#         session = DBSession()
#         item: Base = [self.db_class(**game) for game in data]
#         session.add_all(item)
#         session.commit()
#         session.close()
#         return True

#     def update(self, player_name: str, data: DataObject) -> DataObject:
#         session = DBSession()
#         item: Base = session.query(self.db_class).get(player_name)
#         for key, value in data.items():
#             setattr(item, key, value)
#         session.commit()
#         session.close()
#         return to_dict(item)
#     def delete(self, player_name: str) -> DataObject:
#         session = DBSession()
#         item: Base = session.query(self.db_class).get(player_name)
#         result = to_dict(item)
#         session.delete(item)
#         session.commit()
#         session.close()
#         return result

