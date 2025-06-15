import concurrent
import os
import requests
import io
import time
def ask_twice(player_name: str, year: int, month: int):
    import time
    USER_AGENT = os.getenv('USER_AGENT')
    DOWNLOAD_MONTH = (
        os.getenv("DOWNLOAD_MONTH")
        .replace("{player_name}", player_name)
        .replace("{year}", str(year))
        .replace("{month}", str(month))
    )
    games = requests.get(DOWNLOAD_MONTH,
                         allow_redirects=True,
                         timeout=3,
                         headers = {"User-Agent": USER_AGENT})
    if len(games.content) == 0:
        time.sleep(1)
        games = requests.get(DOWNLOAD_MONTH,
                             allow_redirects=True,
                             timeout=10,
                             headers = {"User-Agent": USER_AGENT})
    if len(games.content) == 0:
        return False
    return games
def download_month(player_name: str, year: int, month: int) -> str:
    """It ask for a month of games of a particular player
    the games package is a pgn_txt.
    Returns:
        io.str
    """
    games = ask_twice(player_name, year, month)
    pgn = io.StringIO(games.content.decode().replace("'", '"'))
    time.sleep(1)
    return pgn
def month_of_games(params: list) -> None:
    """
    Download a month, splits the games and return a list of pgn games.
    """
    pgn = download_month(params["player_name"], params["year"], params["month"])
    if pgn == False:
        params["return_games"].extend(False)
        return
    params["return_games"].extend(pgn.read().split("\n\n\n"))

def download_months(player_name, valid_dates):
    """
    Ask chessdotcom for the games on the date_range trough a threadpool
    returns valid games for valid months and a list of months asked
    """
    return_games = []
    params = [
        {
            "player_name": player_name,
            "year": date[0],
            "month": date[1],
            "return_games": return_games,
        }
        for date in valid_dates
    ]
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        executor.map(month_of_games, params)
    print(f"GOT {len(return_games)} games")
    print("downloading over")
    return_games = [game for game in return_games if game is not False]
    print(f'returned months = {len(return_games)}')
    return return_games