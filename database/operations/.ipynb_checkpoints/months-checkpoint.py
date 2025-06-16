#OPERATIONS
#from .utils.ask_db import player_exists_at_db
#from .chess_com_api import get_profile
from .interface import DataInterface
from .models import MonthCreateData, MonthResult
from fastapi.responses import PlainTextResponse


def html_response(content):
    return PlainTextResponse(content=content, status_code=200)

def read_months(player_name: str, month_interface: DataInterface) -> MonthResult:
    months = month_interface.player_exists(player_name)
    if not months:
        return False
    return MonthResult(**months)

def create_month(
    data: dict, month_interface: DataInterface) -> MonthResult:
    data['player_name'] = data['player_name'].lower()
    month = month_interface.create(MonthCreateData(**data).model_dump())
    return MonthResult(**month)
    
def update_month(
    data: dict, month_interface: DataInterface) -> MonthResult:
    data['player_name'] = data['player_name'].lower()
    month = month_interface.create(MonthCreateData(**data).model_dump())
    return MonthResult(**month)
    
