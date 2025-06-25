# database/operations/players.py
from fastapi.encoders import jsonable_encoder
from typing import Optional, Union, Dict, Any, Tuple
from sqlalchemy.exc import IntegrityError
from database.database.db_interface import DBInterface
from database.database.models import Player
from database.operations.models import PlayerCreateData 
from database.operations.chess_com_api import get_profile

async def read_player(player_name: str) -> Optional[Dict[str, Any]]:
    """
    Reads a player's profile from the database by player_name.
    Args:
        player_name (str): The name of the player to retrieve.
    Returns:
        Optional[Dict[str, Any]]: A dictionary representing the player's profile, or None.
    """
    player_db_interface = DBInterface(Player)
    player_name_lower = player_name.lower()
    player_data = await player_db_interface.read_by_name(player_name_lower)

    if player_data:
        return player_data
    else:
        print(f"Player {player_name_lower} not found in DB.")
        return None

async def insert_player(data: dict) -> Optional[PlayerCreateData]:
    """
    Inserts a new player into the database or updates an existing one if a unique
    constraint violation occurs during insertion (i.e., the player already exists).
    It always attempts to update with the latest profile data fetched from Chess.com.

    Args:
        data (dict): A dictionary containing 'player_name' to process.

    Returns:
        Optional[PlayerCreateData]: The PlayerCreateData model of the inserted/updated player,
                                    or None if processing fails.
    """
    player_name_lower = data['player_name'].lower()
    player_interface = DBInterface(Player)

    # Always attempt to fetch the latest profile from Chess.com API first
    fetched_profile: Optional[PlayerCreateData] = await get_profile(player_name_lower)

    if fetched_profile is None:
        print(f"Failed to fetch profile for {player_name_lower} from Chess.com. Checking existing data.")
        # If profile fetch fails, try to return existing data if available
        existing_player_data_orm = await player_interface.read_by_name(player_name_lower)
        if existing_player_data_orm:
            print(f"Returning existing (possibly incomplete) profile for {player_name_lower}.")
            return PlayerCreateData(**player_interface.to_dict(existing_player_data_orm))
        print(f"No profile fetched and no existing data for {player_name_lower}. Cannot proceed.")
        return None

    fetched_profile_dict = fetched_profile

    try:
        # First, attempt to create a new player record.
        # This is the "upsert" pattern: try insert, if conflict, then update.
        print(f"Attempting to insert new player profile for: {player_name_lower}")
        created_player_orm = await player_interface.create(fetched_profile_dict)
        return PlayerCreateData(**player_interface.to_dict(created_player_orm))
    except IntegrityError: # Catches unique constraint violations (and other integrity errors)
        # If an IntegrityError occurs, it means the player likely already exists.
        # So, we attempt to update the existing record with the new fetched data.
        print(f"Player {player_name_lower} already exists (IntegrityError caught). Attempting to update instead.")
        try:
            updated_player_orm = await player_interface.update_by_name(player_name_lower, fetched_profile_dict)
            return PlayerCreateData(**player_interface.to_dict(updated_player_orm))
        except Exception as update_e:
            print(f"Error updating player {player_name_lower} after failed insert: {update_e}")
            return None
    except Exception as e:
        # Catch any other unexpected exceptions during the create attempt
        print(f"An unexpected error occurred during player creation for {player_name_lower}: {e}")
        return None

# async def insert_player(data: Dict[str, Any]) -> Optional[PlayerCreateData]:
#     """
#     Inserts a player into the database if they don't exist.
#     Args:
#         data (dict()): {"player_name":"chess.com_username"}
#     Returns PlayerCreateData instance on success, None on failure.
#     """
#     player_db_interface = DBInterface(Player)
#     player_name_lower = data['player_name'].lower()
#     existing_player_data = await player_db_interface.player_exists(player_name_lower)
#     if existing_player_data:
#         profile = await read_player(player_name_lower)
#         return PlayerCreateData(**profile)
#     # Player does not exist so...
#     profile = await get_profile(player_name_lower)
#     if profile is None:
#         print(f"Failed to fetch profile for {player_name_lower} from .chess_com_api.")
#         return None
#     try:        
#         profile = PlayerCreateData(**profile)
#         created_player_orm = await player_db_interface.create(profile.model_dump())
#         if created_player_orm:            
#             return profile
#         else:
#             print(f"Error: DBInterface.create returned None for {player_name_lower}.")
#             return None
#     except Exception as e:
#         print(f"Error inserting player {player_name_lower} into database: {e}")
#         return None

async def get_players_already_in_db(player_names: Tuple[str, ...]) -> set:
    """
    Checks the players all ready at DB and filters them to get only new ones.
    Args:
        tuple: ("user_name_1","user_name_n")    
    Returns tuple of new usernames on success, empty for no new players.
    """
    player_interface = DBInterface(Player)
    async with player_interface.AsyncSessionLocal() as session:
        result = await session.execute(
            player_interface.Model.select().filter(player_interface.Model.player_name.in_(player_names))
        )
        return set(player.player_name for player in result.scalars().all())
