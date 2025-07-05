# import time
# from typing import Set

# from sqlalchemy import Column, String, text, select
# from sqlalchemy.ext.asyncio import AsyncSession
# from sqlalchemy.orm import declarative_base


from .models import PlayerCreateData
from database.database.db_interface import DBInterface
from database.database.models import Player
# --- TEMPORARY TABLE MODEL ---
# # This Base is specifically for the temporary table.
# # It helps keep it separate from your main models for migration purposes.
# # If you prefer, you can use your main 'Base' here, but be mindful during migrations.
# TempBase = declarative_base()

# class TempPlayerName(TempBase):
#     """
#     SQLAlchemy model for a temporary table to hold player names for bulk lookups.
#     """
#     __tablename__ = "temp_player_names"
#     # 'TEMPORARY' prefix is for PostgreSQL. Other databases might use different syntax
#     # (e.g., 'TEMP'). For SQLite, it's typically 'TEMP'.
#     __table_args__ = {'prefixes': ['TEMPORARY']}
    
#     player_name_col = Column(String, primary_key=True)


# # --- ASYNC FUNCTION TO GET ONLY NEW PLAYERS ---
# async def get_only_players_not_in_db(player_names: Set[str]) -> Set[str]:
#     """
#     Identifies which player names from the input set do not yet exist in the Player table.
#     Uses a temporary table to efficiently handle large numbers of player names
#     without hitting PostgreSQL's parameter limit for IN clauses or bulk inserts.

#     Args:
#         player_names (set[str]): A set of player names to check against the database.

#     Returns:
#         set[str]: A set of player names that are not found in the database.
#     """
#     # Assuming player_interface is instantiated and provides:
#     # - player_interface.model (which is your Player model class)
#     # - player_interface.AsyncSessionLocal (your async session factory)
#     # You might need to pass player_interface as an argument or make it globally accessible.
    
#     # Placeholder for DBInterface and Player. You'll need to ensure these are defined
#     # and accessible in your environment where this function is placed.
#     # Example:
#     # from database.interfaces import DBInterface # Assuming this path
#     # from database.models import Player         # Assuming this path
    
#     # A simplified placeholder to allow the code to run if DBInterface/Player are not available
#     class MockPlayerModel: # This is just for demonstration if DBInterface isn't globally available
#         player_name = Column(String) # Mock the column needed for the select statement
    
#     class MockDBInterface:
#         def __init__(self, model):
#             self.model = model
#             # In your actual code, AsyncSessionLocal would be configured
#             # e.g., from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
#             # engine = create_async_engine(...)
#             # self.AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
#             self.AsyncSessionLocal = None # Placeholder, replace with your actual AsyncSessionLocal
#             print("WARNING: Mock DBInterface and Player are being used. Ensure real ones are imported.")

#     # You MUST replace this line with your actual DBInterface and Player instance/class setup
#     # Example: player_interface = DBInterface(Player)
#     player_interface = DBInterface(Player) # REMOVE THIS AND UNCOMMENT YOUR ACTUAL DBINTERFACE LINE

#     if not player_names:
#         print("No player names provided for lookup.")
#         return set()

#     player_names_list = list(player_names)
    
#     # A safe batch size for inserting into the temporary table.
#     # This prevents the 'number of query arguments cannot exceed 32767' error.
#     BULK_INSERT_BATCH_SIZE = 5000 

#     players_found_in_db = set()

#     # Use an asynchronous session to perform database operations
#     async with player_interface.AsyncSessionLocal() as session:
#         start_temp_table_ops = time.time()
#         print(f"[{time.time()-start_temp_table_ops:.2f}s] Starting temp table operations...")

#         # Step 1: Create the temporary table.
#         # 'ON COMMIT DROP' ensures it's automatically cleaned up when the transaction finishes.
#         await session.execute(text("""
#             CREATE TEMPORARY TABLE IF NOT EXISTS temp_player_names (
#                 player_name_col VARCHAR PRIMARY KEY
#             ) ON COMMIT DROP;
#         """))
#         print(f"[{time.time()-start_temp_table_ops:.2f}s] Temporary table created.")
        
#         # Step 2: Insert player names into the temporary table in batches.
#         # This prevents hitting the parameter limit during the insert itself.
#         for i in range(0, len(player_names_list), BULK_INSERT_BATCH_SIZE):
#             batch = player_names_list[i : i + BULK_INSERT_BATCH_SIZE]
            
#             # Prepare data for bulk insert into the temporary table
#             temp_data = [{"player_name_col": name} for name in batch]
            
#             # Perform the batched bulk insert
#             await session.execute(
#                 TempPlayerName.__table__.insert(), # Use the Table object for bulk insert
#                 temp_data
#             )
#             print(f"[{time.time()-start_temp_table_ops:.2f}s] Inserted batch {i // BULK_INSERT_BATCH_SIZE + 1} of {len(batch)} players into temp table.")

#         # IMPORTANT: Commit the temporary table creation and batch inserts.
#         # This makes the data in the temporary table visible for the subsequent JOIN query.
#         await session.commit() 
#         print(f"[{time.time()-start_temp_table_ops:.2f}s] All player names inserted into temp table and committed.")

#         # Step 3: Query the main player table by joining with the temporary table.
#         # This is highly efficient and avoids the 'IN' clause parameter limit.
#         start_join_query = time.time()
#         print(f"[{time.time()-start_temp_table_ops:.2f}s] Performing JOIN query to find existing players...")
#         stmt = select(player_interface.model.player_name).join(
#             TempPlayerName,
#             player_interface.model.player_name == TempPlayerName.player_name_col
#         )
        
#         result = await session.execute(stmt)
#         players_found_in_db.update(result.scalars().all())
#         print(f"[{time.time()-start_temp_table_ops:.2f}s] JOIN query completed in {time.time()-start_join_query:.2f}s.")

#     print(f"Total time for get_only_players_not_in_db: {time.time()-start_temp_table_ops:.2f} seconds")
#     # Return the set of players that were in the input but NOT found in the database
#     return player_names - players_found_in_db




import time
from typing import Set
import asyncio
from math import ceil

from sqlalchemy import Column, String, text, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import declarative_base

# --- TEMPORARY TABLE MODEL ---
TempBase = declarative_base()

class TempPlayerName(TempBase):
    """
    SQLAlchemy model for a temporary table to hold player names for bulk lookups.
    """
    __tablename__ = "temp_player_names"
    # Ensure this is correct for your PostgreSQL setup.
    # ON COMMIT DROP is good, but means everything must be in one transaction.
    __table_args__ = {'prefixes': ['TEMPORARY']}

    player_name_col = Column(String, primary_key=True)


# --- ASYNC FUNCTION TO GET ONLY NEW PLAYERS ---
async def get_only_players_not_in_db(player_names: Set[str]) -> Set[str]:
    """
    Identifies which player names from the input set do not yet exist in the Player table.
    Uses a temporary table to efficiently handle large numbers of player names
    without hitting PostgreSQL's parameter limit for IN clauses or bulk inserts.

    Args:
        player_names (set[str]): A set of player names to check against the database.
        player_interface: An instance of DBInterface for your Player model.

    Returns:
        set[str]: A set of player names that are not found in the database.
    """
    player_interface = DBInterface(Player)
    if not player_names:
        print("No player names provided for lookup.")
        return set()

    player_names_list = list(player_names)

    INSERT_VALUES_BATCH_SIZE = 1000 # Keep this relatively small and adjust if needed

    players_found_in_db = set()

    # Use an asynchronous session to perform database operations
    # ALL operations (create, insert, select/join) MUST happen within this single 'async with' block
    # before the implicit commit at the end of the block.
    async with player_interface.AsyncSessionLocal() as session:
        start_temp_table_ops = time.time()
        print(f"[{time.time()-start_temp_table_ops:.2f}s] Starting temp table operations for {len(player_names)} players...")

        # Step 1: Create the temporary table.
        # This occurs within the transaction.
        await session.execute(text("""
            CREATE TEMPORARY TABLE IF NOT EXISTS temp_player_names (
                player_name_col VARCHAR PRIMARY KEY
            ) ON COMMIT DROP;
        """))
        print(f"[{time.time()-start_temp_table_ops:.2f}s] Temporary table created.")

        # Step 2: Insert player names into the temporary table in batches.
        # These inserts also occur within the same transaction.
        insert_tasks = []
        for i in range(0, len(player_names_list), INSERT_VALUES_BATCH_SIZE):
            batch = player_names_list[i : i + INSERT_VALUES_BATCH_SIZE]

            # Construct a multi-value INSERT statement using text() for explicit control
            values_clause = ", ".join([f"('{name.replace("'", "''")}')" for name in batch])
            insert_sql = f"INSERT INTO temp_player_names (player_name_col) VALUES {values_clause};"
            insert_tasks.append(session.execute(text(insert_sql)))

        # Execute all insert tasks concurrently within the current session's transaction
        await asyncio.gather(*insert_tasks)

        print(f"[{time.time()-start_temp_table_ops:.2f}s] All {len(player_names_list)} player names inserted into temp table.")

        # NO explicit session.commit() here!
        # The session.commit() that was here was causing the temporary table to be dropped prematurely.
        # The 'async with' block for AsyncSessionLocal will implicitly commit at its end,
        # ensuring all operations are part of one transaction.

        # Step 3: Query the main player table by joining with the temporary table.
        # This query now runs within the same active transaction where the temp table exists.
        start_join_query = time.time()
        print(f"[{time.time()-start_temp_table_ops:.2f}s] Performing JOIN query to find existing players...")
        stmt = select(player_interface.model.player_name).join(
            TempPlayerName,
            player_interface.model.player_name == TempPlayerName.player_name_col
        )

        result = await session.execute(stmt)
        players_found_in_db.update(result.scalars().all())
        print(f"[{time.time()-start_temp_table_ops:.2f}s] JOIN query completed in {time.time()-start_join_query:.2f}s.")

    # The 'async with session:' block concludes here.
    # If successful, it performs an implicit commit, at which point
    # the temporary table will be dropped due to ON COMMIT DROP.
    # If an exception occurs, it will implicitly roll back.

    print(f"Total time for get_only_players_not_in_db: {time.time()-start_temp_table_ops:.2f} seconds")
    return player_names - players_found_in_db