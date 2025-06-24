# database/database/engine.py (FINAL REVISION for init_db)

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
# Removed: from sqlalchemy_utils import database_exists, create_database # <--- NO LONGER USED
from .models import Base
from constants import CONN_STRING
import asyncio
import asyncpg # Import asyncpg directly for low-level DB management
from urllib.parse import urlparse # To parse the connection string

# Define engine and sessionmaker globally
async_engine = None
AsyncDBSession = sessionmaker(expire_on_commit=False, class_=AsyncSession)

async def init_db(connection_string: str):
    global async_engine

    # Parse the connection string to get details for asyncpg
    parsed_url = urlparse(connection_string)
    db_user = parsed_url.username
    db_password = parsed_url.password
    db_host = parsed_url.hostname
    db_port = parsed_url.port
    db_name = parsed_url.path.lstrip('/') # Get the target database name

    # --- Step 1: Check and Create Database using asyncpg directly ---
    # We'll connect to the default 'postgres' database to create the target database if it doesn't exist.
    temp_conn = None # Initialize to None for finally block
    try:
        # Connect to the default 'postgres' database to check/create the target database
        temp_conn = await asyncpg.connect(
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
            database='postgres' # Connect to a default database to perform creation
        )
        
        # Check if the target database exists
        db_exists_query = f"SELECT 1 FROM pg_database WHERE datname='{db_name}'"
        db_exists = await temp_conn.fetchval(db_exists_query)

        if not db_exists:
            print(f"Database '{db_name}' does not exist. Creating...")
            # IMPORTANT: Cannot use f-strings directly with DDL for security and quoting.
            # However, database names often don't need parameters. If `db_name` can come from
            # untrusted input, you'd need careful quoting. For typical env vars, it's safer.
            await temp_conn.execute(f'CREATE DATABASE "{db_name}"') # Use double quotes for identifier
            print(f"Database '{db_name}' created.")
        else:
            print(f"Database '{db_name}' already exists.")

    except asyncpg.exceptions.DuplicateDatabaseError:
        # This can happen if another process creates it concurrently. Harmless.
        print(f"Database '{db_name}' already exists (concurrent creation).")
    except Exception as e:
        print(f"Error during database existence check/creation: {e}")
        # Depending on severity, you might want to re-raise or handle gracefully
        raise
    finally:
        if temp_conn:
            await temp_conn.close() # Ensure the temporary connection is closed

    # --- Step 2: Initialize SQLAlchemy Async Engine and Create Tables ---
    # This part remains mostly the same as before, as it was correct for the AsyncEngine
    
    # Create the main asynchronous engine for the target database
    async_engine = create_async_engine(connection_string, echo=False)
    
    # Run Base.metadata.create_all asynchronously using the async engine
    async with async_engine.begin() as conn:
        print("Ensuring database tables exist...")
        # Base.metadata.create_all is synchronous, must be run in a sync context (which run_sync provides)
        await conn.run_sync(Base.metadata.create_all)
        print("Database tables checked/created.")
    
    # Configure the async sessionmaker with the new engine
    AsyncDBSession.configure(bind=async_engine)
    print("Database initialization complete.")



# sync_engine: Engine = None
# DBSession = sessionmaker()

# def sync_init_db(sync_connection_string: str):
    
#     url = sync_connection_string
#     if not database_exists(url):
#         create_database(url)
#     sync_engine = create_engine(url)
#     Base.metadata.create_all(bind=sync_engine)
#     DBSession.configure(bind=sync_engine)


# # database/database/engine.py (REVISED)

# from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
# from sqlalchemy.orm import sessionmaker
# from sqlalchemy_utils import database_exists, create_database
# from .models import Base
# from constants import CONN_STRING
# import asyncio # Import asyncio
# from sqlalchemy import text # For explicit text() usage if needed, though database_exists wraps it

# async_engine = None
# AsyncDBSession = sessionmaker(expire_on_commit=False, class_=AsyncSession)

# async def init_db(connection_string: str):
#     global async_engine
#     url = connection_string

#     # --- FIX: Wrap synchronous database_exists and create_database calls in asyncio.to_thread ---
#     # These functions perform I/O that is synchronous.
#     # We must run them in a separate thread to avoid blocking the asyncio event loop.
    
#     # Check if database exists synchronously in a thread
#     db_exists = await asyncio.to_thread(database_exists, url)
#     if not db_exists:
#         # Create database synchronously in a thread
#         print(f"Database '{url.split('/')[-1]}' does not exist. Creating...")
#         await asyncio.to_thread(create_database, url)
#         print(f"Database '{url.split('/')[-1]}' created.")
#     else:
#         print(f"Database '{url.split('/')[-1]}' already exists.")

#     # Create the asynchronous engine
#     async_engine = create_async_engine(url, echo=False) # echo=True can be useful for debugging SQL
    
#     # Run Base.metadata.create_all asynchronously using the async engine
#     # conn.run_sync is already designed to run synchronous code in a thread pool managed by SQLAlchemy
#     # so no need for asyncio.to_thread here for create_all itself.
#     async with async_engine.begin() as conn:
#         print("Ensuring database tables exist...")
#         await conn.run_sync(Base.metadata.create_all) # Base.metadata.create_all is a synchronous SQLAlchemy operation
#         print("Database tables checked/created.")
    
#     # Configure the async sessionmaker with the new engine
#     AsyncDBSession.configure(bind=async_engine)
#     print("Database initialization complete.")