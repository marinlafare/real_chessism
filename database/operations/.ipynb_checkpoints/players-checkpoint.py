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

    fetched_profile: Optional[PlayerCreateData] = await get_profile(player_name_lower)

    if fetched_profile is None:
        print('player_name_lower has to be incorrect, the profile from chess.com came back as None')
        return None

    fetched_profile_dict = fetched_profile

    try:
        print(f"Attempting to insert new player profile for: {player_name_lower}")
        created_player_orm = await player_interface.create(fetched_profile_dict)
        print(f'NEW player {player_name_lower} inserted')
        return PlayerCreateData(**player_interface.to_dict(created_player_orm))
    except IntegrityError: # Catches unique constraint violations (and other integrity errors)
        # Means player exists
        # So, we attempt to update the existing record with the new fetched data.
        print(f"Updating {player_name_lower}.")
        try:
            updated_player_orm = await player_interface.update_by_name(player_name_lower, fetched_profile_dict)
            return PlayerCreateData(**player_interface.to_dict(updated_player_orm))
        except Exception as update_e:
            print(f"Error updating player {player_name_lower} after failed insert: {update_e}")
            return None
    except Exception as e:
        print(f"An unexpected error occurred during player creation for {player_name_lower}: {e}")
        return None
