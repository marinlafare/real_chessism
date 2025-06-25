# database/database/engine.py

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from .models import Base
from constants import CONN_STRING
import asyncio
import asyncpg
from urllib.parse import urlparse

async_engine = None
AsyncDBSession = sessionmaker(expire_on_commit=False, class_=AsyncSession)

async def init_db(connection_string: str):
    global async_engine

    parsed_url = urlparse(connection_string)
    db_user = parsed_url.username
    db_password = parsed_url.password
    db_host = parsed_url.hostname
    db_port = parsed_url.port
    db_name = parsed_url.path.lstrip('/')

    temp_conn = None
    try:
        # database_exists and create_database are from sqlalchemy_utils whichs works for non-async dbs
        # So this has to be done.
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
            await temp_conn.execute(f'CREATE DATABASE "{db_name}"')
            print(f"Database '{db_name}' created.")
        else:
            print(f"Database '{db_name}' already exists.")

    except asyncpg.exceptions.DuplicateDatabaseError:
        print(f"Database '{db_name}' already exists (concurrent creation).")
    except Exception as e:
        print(f"Error during database existence check/creation: {e}")
        raise
    finally:
        if temp_conn:
            await temp_conn.close() # Ensure the temporary connection is closed

    async_engine = create_async_engine(connection_string, echo=False)
    
    async with async_engine.begin() as conn:
        print("Ensuring database tables exist...")
        await conn.run_sync(Base.metadata.create_all)
        print("Database tables checked/created.")
    
    AsyncDBSession.configure(bind=async_engine)
    print("Database initialization complete.")
