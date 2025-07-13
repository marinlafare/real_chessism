#OPERATIONS

from .months import read_months, create_month
from .players import read_player
from .interface import DataInterface
from datetime import datetime
import pandas as pd
from database.database.db_interface import DBInterface
from database.database.models import Player, Game, Month
player_interface = DBInterface(Player)
game_interface = DBInterface(Game)
month_interface = DBInterface(Month)
#recorded_dates = read_months(player_name, month_interface)
#create_month({'player_name':'hikaru','dates':'1_2_3'},month_interface)

def just_new_dates(player_name, dates):
    recorded_dates = read_months(player_name, month_interface)
    if not recorded_dates:
        return dates
    return recorded_dates
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
    join_at = read_player(player_name, player_interface).joined
    join_at_year = datetime.fromtimestamp(join_at).year
    join_at_month = datetime.fromtimestamp(join_at).month
    current_date = datetime.now().year, datetime.now().month
    return join_at_year, join_at_month, current_date[0], current_date[1]

def create_date_range(dates) -> list[tuple]:
    """
    Takes a str with format: from_year-from_month-to_year-to-month
    returns the range as a list of tuples:
    input: 2020, 1, 2020, 3 = output: [(2020,1),(2020,2),(2020,3)]
    """
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
        return specific_range(player_name,
                              date_petition)
def specific_range(player_name: str,
                   date_petition: str) -> list:
    
    dates = format_date_petition(date_petition)
    if type(dates) == str:
        return dates
    range_dates = create_date_range(dates)
    return range_dates
def full_range(player_name: str) -> list:
    dates = get_joined_and_current_date(player_name)
    range_dates = create_date_range(dates)
    return range_dates
