# operations/chess_com_api.py

import asyncio
import httpx
import json
import time
from typing import List, Tuple, Dict, Any, Optional

# Ensure these imports are correct based on your project structure
from constants import USER_AGENT # Assuming USER_AGENT is defined here
import database.operations.chess_com_endpoints as chess_com_endpoints
from database.operations.models import PlayerCreateData

# --- API CLIENT FUNCTIONS ---

async def get_profile(player_name: str) -> Optional[PlayerCreateData]:
    """
    Fetches a player's profile from the Chess.com API and returns it as a
    PlayerCreateData Pydantic model instance.

    Args:
        player_name (str): The Chess.com username.

    Returns:
        PlayerCreateData | None: An instance of PlayerCreateData with the profile details,
                                 or None if whateva.
    """
    PLAYER_URL = chess_com_endpoints.PLAYER.replace('{player}', player_name)
    async with httpx.AsyncClient(timeout=5) as client:
        try:
            response = await client.get(
                PLAYER_URL,
                headers={"User-Agent": USER_AGENT}
            )
            response.raise_for_status()
            
            raw_data = response.json()

            # --- Transformation to match PlayerCreateData Pydantic model ---
            processed_data = {} 
            processed_data['player_name'] = player_name.lower() # Always use the requested player_name

            # Populate other fields using .get() for safety and let Pydantic handle defaults
            processed_data['name'] = raw_data.get('name')
            processed_data['url'] = raw_data.get('url')
            processed_data['title'] = raw_data.get('title')
            processed_data['avatar'] = raw_data.get('avatar')
            processed_data['followers'] = raw_data.get('followers')
            
            country_url = raw_data.get('country')
            if country_url:
                processed_data['country'] = country_url.split('/')[-1]
            else:
                processed_data['country'] = None

            processed_data['location'] = raw_data.get('location')
            
            joined_ts = raw_data.get('joined')
            if joined_ts is not None:
                try:
                    processed_data['joined'] = int(joined_ts)
                except (ValueError, TypeError):
                    print(f"Warning: Could not convert 'joined' ({joined_ts}) to int for {player_name}. Setting to 0.")
                    processed_data['joined'] = 0
            else:
                processed_data['joined'] = 0
            try:
                
                processed_data['status'] = raw_data.get('status')
                processed_data['is_streamer'] = raw_data.get('is_streamer')
                processed_data['twitch_url'] = raw_data.get('twitch_url')
                processed_data['verified'] = raw_data.get('verified')
                processed_data['league'] = raw_data.get('league')
                print('raw data',processed_data)
                player_data = processed_data
                return player_data # Return the Pydantic model instance
            except Exception as pydantic_error:
                print(f"Pydantic validation error for {player_name}: {pydantic_error}")
                print(f"Data that failed validation: {processed_data}")
                return None

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                print(f"Player '{player_name}' not found on Chess.com (404).")
                return None
            print(f"HTTP error for profile {player_name}: {e.response.status_code} - {e.response.text}")
            return None
        except httpx.RequestError as e:
            print(f"Request error for profile {player_name}: {e}")
            return None
        except Exception as e:
            print(f"An unexpected error occurred getting profile {player_name}: {e}")
            return None


async def ask_twice(player_name: str, year: int, month: int, client: httpx.AsyncClient) -> Optional[httpx.Response]:
    """
    Fetches game archives for a specific month, with a retry logic.
    Uses an existing httpx.AsyncClient instance.

    Args:
        player_name (str): The Chess.com username.
        year (int): The year of the archive.
        month (int): The month of the archive.
        client (httpx.AsyncClient): The shared HTTPX async client.

    Returns:
        httpx.Response | None: The HTTPX Response object if successful, None otherwise.
    """
    # Ensure month is two digits for URL formatting
    month_str = f"{month:02d}"

    DOWNLOAD_MONTH_URL = (
        chess_com_endpoints.DOWNLOAD_MONTH
        .replace("{player}", player_name)
        .replace("{year}", str(year))
        .replace("{month}", month_str)
    )

    try:
        games_response = await client.get(
            DOWNLOAD_MONTH_URL,
            follow_redirects=True,
            timeout=5,
            headers={"User-Agent": USER_AGENT}
        )
        
        # Check for empty content on first try and retry if necessary
        if not games_response.content:
            await asyncio.sleep(1)
            games_response = await client.get(
                DOWNLOAD_MONTH_URL,
                follow_redirects=True,
                timeout=10,
                headers={"User-Agent": USER_AGENT}
            )
        
        if not games_response.content:
            print(f"No content after two attempts for {player_name} in {year}-{month_str}.")
            return None

        games_response.raise_for_status()
        return games_response

    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            print(f"No games found for {player_name} in {year}-{month_str} (404).")
            return None
        print(f"HTTP error downloading month {year}-{month_str}: {e.response.status_code} - {e.response.text}")
        return None
    except httpx.RequestError as e:
        print(f"Request error downloading month {year}-{month_str}: {e}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred downloading month {year}-{month_str}: {e}")
        return None


async def download_month(player_name: str, year: int, month: int, client: httpx.AsyncClient) -> Optional[httpx.Response]:
    """
    Wrapper for ask_twice to get a month's games, passing the shared client.
    """
    games = await ask_twice(player_name, year, month, client)
    return games


async def month_of_games(param: Dict[str, Any], client: httpx.AsyncClient) -> Optional[Dict[str, Any]]:
    """
    Downloads a month of games and returns as parsed JSON dictionary.
    Passes the shared httpx.AsyncClient.

    Args:
        param (Dict[str, Any]): Dictionary containing 'player_name', 'year', 'month'.
        client (httpx.AsyncClient): The shared HTTPX async client.

    Returns:
        Dict[str, Any] | None: The parsed JSON dictionary containing game data, or None on failure.
    """
    player_name = param["player_name"]
    year = param["year"]
    month = param["month"]

    pgn_response = await download_month(player_name, year, month, client)
    
    if pgn_response is None:
        return None

    try:
        text_games = pgn_response.text
        text_games = text_games.replace(' \\"Let"s Play!','lets_play') 
        
        parsed_json = json.loads(text_games)
        return parsed_json
    except json.JSONDecodeError as e:
        print(f'JSON decoding failed for year: {year}, month: {month}: {e}')
        return None
    except Exception as e:
        print(f"Error filtering raw PGN for {year}-{month}: {e}")
        return None


async def download_months(
                    player_name: str,
                    valid_dates: List[str],
                    max_concurrent_requests: int = 2,
                    min_delay_between_requests: float = 0.5
                        ) -> Dict[int, Dict[int, List[Dict[str, Any]]]]:
    """
    Downloads games for a player's month strings with controlled concurrency.

    Args:
        player_name (str): chess.com player's username.
        valid_dates (List[str]): A list of 'YYYY-MM' strings to download.
        max_concurrent_requests (int): The maximum number of simultaneous requests to make.
                                      Adjust based on Chess.com API rate limits.
        min_delay_between_requests (float): Minimum delay in seconds between starting new requests.
                                            This is crucial for API rate limiting.

    Returns:
        Dict[int, Dict[int, List[Dict[str, Any]]]]: A dictionary where keys are years,
        nested keys are months, and values are lists of game dictionaries.
    """
    all_games_by_month: Dict[int, Dict[int, List[Dict[str, Any]]]] = {}
    
    semaphore = asyncio.Semaphore(max_concurrent_requests)

    async with httpx.AsyncClient(timeout=15) as shared_client:
        async def fetch_month_games_task(month_str: str) -> Tuple[int, int, Optional[List[Dict[str, Any]]]]:
            """
            Internal async function to fetch games for a single month,
            semaphore and delay to not upset chess_com.
            
            Arg: mont_str = '2020-1'
            
            Returns: A tuple (year, month, games_list) or (year, month, None) on error/no games.
            """
            async with semaphore:
                await asyncio.sleep(min_delay_between_requests)

                year_str, month_str_val = month_str.split('-')
                year = int(year_str)
                month = int(month_str_val)

                param = {"player_name": player_name, "year": year, "month": month}
                
                result = await month_of_games(param, shared_client) 

                if result is None:
                    return year, month, None
                
                if 'games' in result and result['games'] is not None:
                    return year, month, result['games']
                else:
                    print(f"No games or invalid data for {year}-{month} (missing/empty 'games' key in parsed JSON).")
                    return year, month, None


        tasks = [fetch_month_games_task(month_str) for month_str in valid_dates]

        # Run tasks concurrently, limited by the semaphore
        print(f"Starting download of {len(tasks)} months with {max_concurrent_requests} concurrent requests...")
        start_time = time.time()
        
        # return_exceptions=True so that one task failing doesn't stop others
        results = await asyncio.gather(*tasks, return_exceptions=True) 

        end_time = time.time()
        print(f"Finished downloading {len(tasks)} months in {end_time - start_time:.2f} seconds.")

    # --- FIX: Handle exceptions in results list before unpacking ---
    for item in results:
        if isinstance(item, Exception):
            print(f"An error occurred in month: {item}")
            continue
        year, month, games_list = item 

        if games_list is not None:
            if year not in all_games_by_month:
                all_games_by_month[year] = {}
            all_games_by_month[year][month] = games_list

    return all_games_by_month

