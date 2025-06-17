from typing import Any

from .engine import DBSession
from .models import Base, to_dict
from sqlalchemy import exists
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy import select

DataObject = dict[str, Any]


class DBInterface:
    def __init__(self, db_class: type[Base],session_factory: sessionmaker):
        self.db_class = db_class
        self.session_factory = session_factory

    def player_exists(self, player_name: str) -> DataObject:
        session = DBSession()
        item: Base = session.query(exists().\
        where(self.db_class.player_name==player_name)).scalar()
        if str(item) == 'True':
            return True
        else:
            return False
    def read_by_name(self, player_name: str)->DataObject:
        session = DBSession()
        data: Base = session.query(self.db_class).get(player_name)
        session.close()
        if data == None:
            return None
        return to_dict(data)
    def create(self, data: DataObject) -> DataObject:
        with self.session_factory() as session:
            try:
                new_item = self.model(**data)
                session.add(new_item)
                session.commit() # Commit the transaction
                session.refresh(new_item) # Refresh the object to get its database-generated ID, etc.
                return new_item
            except Exception as e:
                session.rollback() # Rollback on error
                print(f"Error creating {self.model.__tablename__} record: {e}")
                raise e # Re-raise the exception after rollback
        
        # session = DBSession()
        # item: Base = self.db_class(**data)
        # session.add(item)
        # session.commit()
        # result = to_dict(item)
        # session.close()
        # return result
    
    def create_all(self, data: DataObject) -> DataObject:
        session = DBSession()
        item: Base = [self.db_class(**game) for game in data]
        session.add_all(item)
        session.commit()
        session.close()
        return True

    def update(self, player_name: str, data: DataObject) -> DataObject:
        session = DBSession()
        item: Base = session.query(self.db_class).get(player_name)
        for key, value in data.items():
            setattr(item, key, value)
        session.commit()
        session.close()
        return to_dict(item)
    def delete(self, player_name: str) -> DataObject:
        session = DBSession()
        item: Base = session.query(self.db_class).get(player_name)
        result = to_dict(item)
        session.delete(item)
        session.commit()
        session.close()
        return result



class DBInterface:
    def __init__(self, db_class: type[Base]):
        self.db_class = db_class
    def __init__(self, model, session_factory: sessionmaker):
        """
        Initializes the DBInterface.

        Args:
            model: The SQLAlchemy model this interface will operate on (e.g., Player, Game).
            session_factory: The sessionmaker configured with the engine (e.g., DBSession from engine.py).
        """
        self.model = model
        self.session_factory = session_factory # Store the sessionmaker

    def create(self, data: dict):
        """
        Creates a new record in the database for the given model.

        Args:
            data (dict): Dictionary containing data for the new record.

        Returns:
            The newly created SQLAlchemy model instance.
        """
        # Use 'with' statement to ensure the session is properly closed
        # and the connection is returned to the pool, even if errors occur.
        with self.session_factory() as session:
            try:
                new_item = self.model(**data)
                session.add(new_item)
                session.commit() # Commit the transaction
                session.refresh(new_item) # Refresh the object to get its database-generated ID, etc.
                return new_item
            except Exception as e:
                session.rollback() # Rollback on error
                print(f"Error creating {self.model.__tablename__} record: {e}")
                raise e # Re-raise the exception after rollback

    def player_exists(self, player_name: str) -> bool:
        """
        Checks if a player with the given name exists in the database.

        Args:
            player_name (str): The name of the player to check.

        Returns:
            bool: True if the player exists, False otherwise.
        """
        with self.session_factory() as session:
            # Efficiently check for existence by selecting a single column (e.g., primary key)
            stmt = select(self.model.player_name).filter_by(player_name=player_name)
            # scalar_one_or_none() returns the first result or None if no results
            result = session.execute(stmt).scalar_one_or_none()
            return result is not None

    def read_by_name(self, player_name: str):
        """
        Reads a player profile from the database by name.

        Args:
            player_name (str): The name of the player to retrieve.

        Returns:
            The SQLAlchemy model instance if found, otherwise None.
        """
        with self.session_factory() as session:
            stmt = select(self.model).filter_by(player_name=player_name)
            result = session.execute(stmt).scalar_one_or_none()
            return result
