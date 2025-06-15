from typing import Any

from .engine import DBSession
from .models import Base, to_dict
from sqlalchemy import exists
DataObject = dict[str, Any]


class DBInterface:
    def __init__(self, db_class: type[Base]):
        self.db_class = db_class

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
        session = DBSession()
        item: Base = self.db_class(**data)
        session.add(item)
        session.commit()
        result = to_dict(item)
        session.close()
        return result
    
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