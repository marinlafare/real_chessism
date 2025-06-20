#OPERATIONS FORMAT_DATES

import pandas as pd
from datetime import datetime
from database.operations.players import insert_player
from database.database.ask_db import open_request

def get_joined_and_current_date(player_name):
    join_at = insert_player({"player_name":player_name})['joined']
    join_at_year = datetime.fromtimestamp(join_at).year
    join_at_month = datetime.fromtimestamp(join_at).month
    current_date = datetime.now().year, datetime.now().month
    return join_at_year, join_at_month, current_date[0], current_date[1]
def full_range(player_name: str) -> list:
    dates = get_joined_and_current_date(player_name)
    range_dates = create_date_range(dates)
    return range_dates
def create_range(player_name: str) -> list:
    return full_range(player_name)

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

def just_new_months(player_name):
    date_range = create_range(player_name)
    recorded_months = open_request(f"select * from months where player_name = '{player_name}'")
    recorded_months = [(x[2],x[3]) for x in recorded_months]
    
    data_range = set(date_range)
    recorded_months = set(recorded_months)
    valid = data_range - recorded_months
    if len(valid) == 0:
        return False
    else:
        return valid




# def specific_range(date_petition: str) -> list:
#     dates = format_date_petition(date_petition)
#     if type(dates) == str:
#         return dates
#     range_dates = create_date_range(dates)
#     return range_dates
# def format_date_petition(dates):
#     dates = validate_dates(dates)
#     if not dates: return 'Not a valid format_date: YYYY_MM-YYYY_MM'
#     from_year = dates.split('-')[0].split('_')[0]
#     from_month = dates.split('-')[0].split('_')[1]
#     to_year = dates.split('-')[1].split('_')[0]
#     to_month = dates.split('-')[1].split('_')[1]
#     print('from_year', from_year)
#     print('from_month',from_month)
#     print('to_year', to_year)
#     print('to_month',to_month)
#     if not int(from_year) <= int(to_year):
#         return 'from_year is bigger than to_year'
#     if int(from_year) == int(to_year):
#         if not int(from_month) <= int(to_month):
#             return 'from_month is bigger than to_month'
#     return from_year, from_month, to_year, to_month
# def validate_dates(dates):
#     if len(dates) not in [15,14,13]:
#         return False
#     if dates.count('_') != 2:
#         return False
#     if dates.count('-') != 1:
#         return False
#     return dates
    
# def format_months(player_name):
    
#     for date in date_range:
#         format = f'{date[0]}_{date[1]}###'
#         new_months += format
#     return new_months