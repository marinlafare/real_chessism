#OPERATIONS

#from .months import read_months, create_month
from .players import read_player
#from .interface import DataInterface
from datetime import datetime
import pandas as pd
from .models import PlayerCreateData
from database.database.db_interface import DBInterface
from database.database.ask_db import open_request
from database.database.models import Player, Game, Month
from database.operations.chess_com_api import get_profile
player_interface = DBInterface(Player)
game_interface = DBInterface(Game)
month_interface = DBInterface(Month)
def format_months(date_range):
    new_months = ''
    for date in date_range:
        format = f'{date[0]}_{date[1]}###'
        new_months += format
    return new_months
def just_new_dates(player_name, date_range):
    if not player_interface.player_exists(player_name):
        player_profile = get_profile(player_name)
        print(player_profile)
        player_profile['player_name'] = player_name
        player_data = PlayerCreateData(**player_profile)
        player_interface.create(player_data.model_dump())
    months = open_request(
        f"select dates from months where months.player_name = '{player_name}'"
        )
    if not months:
        new_months = format_months(date_range)
        month_interface.create({"player_name":player_name,"dates":new_months})
        return date_range
    else:
        months = months[0][0]
        print('month dates', months)
        months_split = [x for x in months.split('###') if len(x)>0]
        old_range = [
            (
                int(x.split('_')[0]),int(x.split('_')[1])
            )
            for x in months_split
            ]
        valid_range = [x for x in date_range if x not in old_range]
        new = list(set(old_range + date_range))
        new_months = format_months(new)
        month_interface.delete(player_name)
        month_interface.create({"player_name":player_name,"dates":new_months})
        if len(valid_range) == 0:
            return 'NO NEW MONTHS TO DOWNLOAD'
        return valid_range

def validate_dates(dates):
    if len(dates) not in [15,14,13]:
        return False
    if dates.count('_') != 2:
        return False
    if dates.count('-') != 1:
        return False
    return dates
    
def format_date_petition(dates):
    dates = validate_dates(dates)
    if not dates: return 'Not a valid format_date: YYYY_MM-YYYY_MM'
    from_year = dates.split('-')[0].split('_')[0]
    from_month = dates.split('-')[0].split('_')[1]
    to_year = dates.split('-')[1].split('_')[0]
    to_month = dates.split('-')[1].split('_')[1]
    print('from_year', from_year)
    print('from_month',from_month)
    print('to_year', to_year)
    print('to_month',to_month)
    if not int(from_year) <= int(to_year):
        return 'from_year is bigger than to_year'
    if int(from_year) == int(to_year):
        if not int(from_month) <= int(to_month):
            return 'from_month is bigger than to_month'
    return from_year, from_month, to_year, to_month
    
def get_joined_and_current_date(player_name):
    join_at = get_profile(player_name)['joined']
    join_at_year = datetime.fromtimestamp(join_at).year
    join_at_month = datetime.fromtimestamp(join_at).month
    current_date = datetime.now().year, datetime.now().month
    return join_at_year, join_at_month, current_date[0], current_date[1]

def create_date_range(dates) -> list[tuple]:
    from_year, from_month, to_year, to_month = dates
    date_range = (
        pd.date_range(
            f"{from_year}-{from_month}-01", f"{to_year}-{to_month}-01", freq="MS"
        )
        .strftime("%Y-%m")
        .tolist()
    )
    date_range = [(int(x.split("-")[0]), int(x.split("-")[1])) for x in date_range]
    return date_range
def create_range(data: dict) -> list:
    player_name = data['player_name']
    date_petition = data['dates']
    if date_petition == 'full':
        return full_range(player_name)
    else:
        return specific_range(date_petition)
def specific_range(date_petition: str) -> list:
    dates = format_date_petition(date_petition)
    if type(dates) == str:
        return dates
    range_dates = create_date_range(dates)
    return range_dates
def full_range(player_name: str) -> list:
    dates = get_joined_and_current_date(player_name)
    range_dates = create_date_range(dates)
    return range_dates
