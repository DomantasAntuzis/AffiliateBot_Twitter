from requests import post
import json
import os
import time
import sys

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.db_connect import get_connection, execute_query, execute_many
from utils.logger import logger


TWITCH_CLIENT_ID = os.getenv("TWITCH_CLIENT_ID")
TWITCH_CLIENT_SECRET = os.getenv("TWITCH_CLIENT_SECRET")

def get_access_token():
    """
    Get access token from Twitch API. Authentication
    """

    token_url = "https://id.twitch.tv/oauth2/token"
    token_params = {
        "client_id": TWITCH_CLIENT_ID,
        "client_secret": TWITCH_CLIENT_SECRET,
        "grant_type": "client_credentials"
    }
    
    token_response = post(token_url, params=token_params)
    token_data = token_response.json()
    access_token = token_data.get("access_token")
    
    if not access_token:
        raise Exception(f"Failed to obtain access token: {token_data}")
    
    return access_token

def fetch_all_genres():
    """
    Fetch all genres from IGDB API and insert into database.
    """

    logger.info("Starting fetch_all_genres()")

    access_token = get_access_token()
    
    headers = {
            "Client-ID": TWITCH_CLIENT_ID,
            "Authorization": f"Bearer {access_token}"
    }

    offset = 0
    limit = 500
    batch_count = 0
    all_genres = []

    connection = get_connection()

    while True:
        query = (
            "fields id,name;"
            f"limit {limit};"
            f"offset {offset};"
        )
        response = post('https://api.igdb.com/v4/genres', headers=headers, data=query)

        if response.status_code != 200:
            print(f"Error {response.status_code}: {response.text}")
            break

        genres_batch = response.json()

        if not genres_batch:
            break

        for genre in genres_batch:
            execute_query("INSERT INTO genres (id, name) VALUES (%s, %s) ON DUPLICATE KEY UPDATE name = VALUES(name)", (genre['id'], genre['name'],), fetch=False, connection=connection)
            
        # Commit after each batch
        connection.commit()
        batch_count += 1

        offset += limit

        if len(genres_batch) < limit:
            break

        time.sleep(0.3)

    connection.close()
    logger.info("Finished fetch_all_genres()")
    return None

def fetch_all_igdb_games():
    
    """
    Fetch all games from IGDB API.

        Params:
        game_type = 0 (base game), 
        platforms = 6 (PC)
    """

    logger.info("Starting fetch_all_igdb_games()")

    access_token = get_access_token()
    
    headers = {
        "Client-ID": TWITCH_CLIENT_ID,
        "Authorization": f"Bearer {access_token}"
    }
    
    all_games = [] 
    offset = 0
    limit = 500
    batch_count = 0
    
    connection = get_connection()
    genres_result = execute_query("SELECT id, name FROM genres", fetch=True, connection=connection)
    valid_genre_ids = set(row[0] for row in genres_result) if genres_result else set()
    
    while True:

        query = (
            "fields name,genres,external_games,platforms;"
            f"where game_type = 0 & release_dates.platform = 6;"
            f"limit {limit};"
            f"offset {offset};"
        )
        
        try:
            response = post('https://api.igdb.com/v4/games', headers=headers, data=query)
            
            if response.status_code != 200:
                print(f"Error {response.status_code}: {response.text}")
                if response.status_code == 429:  # Rate limit
                    time.sleep(1)
                    continue
                break
            
            games_batch = response.json()
            #Write to database
            
            # If no games returned, we've reached the end
            if not games_batch:
                break

            # Fetch all item_ids that already have genres assigned (once per batch)
            items_with_genres_result = execute_query("SELECT DISTINCT item_id FROM item_genres", fetch=True, connection=connection)
            items_with_genres = set(row[0] for row in items_with_genres_result) if items_with_genres_result else set()

            # Prepare batch data for items
            items_to_insert = []
            igdb_ids_to_query = []

            for game in games_batch:
                items_to_insert.append((game['name'], "base game", game["id"]))
                igdb_ids_to_query.append(game["id"])

            # Batch insert all items at once
            try:
                execute_many(
                    "INSERT INTO items (title, item_type, igdb_id) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE title = VALUES(title)",
                    items_to_insert,
                    connection=connection,
                    buffered=True
                )
                
                print(f"Batch inserted {len(items_to_insert)} games")
                
                # Batch query to get all item_ids for this batch
                format_strings = ','.join(['%s'] * len(igdb_ids_to_query))
                items_results = execute_query(
                    f"SELECT * FROM items WHERE igdb_id IN ({format_strings})",
                    tuple(igdb_ids_to_query),
                    fetch=True,
                    connection=connection,
                    buffered=True
                )
                
                # Create mapping: igdb_id -> item_id (primary key is first column, igdb_id is 4th column)
                # Adjust indices based on your actual table schema
                igdb_to_item_map = {}
                for row in items_results:
                    item_id = row[0]  # First column is primary key
                    igdb_id = row[3] if len(row) > 3 else row[2]  # Adjust based on schema
                    igdb_to_item_map[igdb_id] = item_id
                
                # Prepare genre inserts
                genre_inserts = []
                
                for game in games_batch:
                    igdb_id = game["id"]
                    item_id = igdb_to_item_map.get(igdb_id)
                    
                    if not item_id:
                        print(f"Warning: Could not find item_id for game {game.get('name', 'unknown')}")
                        continue
                    
                    # Check if genres already assigned
                    if item_id not in items_with_genres:
                        game_genres = game.get("genres", [])
                        if game_genres:
                            for genre_id in game_genres:
                                # Only add if genre exists in valid_genre_ids
                                if genre_id in valid_genre_ids:
                                    genre_inserts.append((item_id, genre_id))
                                else:
                                    print(f"Warning: Genre ID {genre_id} not found in database for game {game.get('name', 'unknown')}")
                        
                        # Mark as processed
                        items_with_genres.add(item_id)
                
                # Batch insert all genres at once
                if genre_inserts:
                    execute_many(
                        "INSERT INTO item_genres (item_id, genre_id) VALUES (%s, %s) ON DUPLICATE KEY UPDATE item_id = item_id",
                        genre_inserts,
                        connection=connection,
                        buffered=True
                    )
                    print(f"Batch inserted {len(genre_inserts)} genre relationships")
                
                # Single commit for entire batch
                connection.commit()
                print(f"Committed batch {batch_count + 1}")
                
            except Exception as e:
                print(f"ERROR processing batch: {e}")
                logger.error(f"ERROR processing batch: {e}")
                import traceback
                traceback.print_exc()
                connection.rollback()

            all_games.extend(games_batch)
            batch_count += 1
            
            print(f"Batch {batch_count}: Total games collected: {len(all_games)}")
            print("-" * 60)
                        
            time.sleep(0.3)  
            
            # Safety check - if we get less than limit, we're done
            if len(games_batch) < limit:
                break
            
            offset += limit
                
        except Exception as e:
            print(f"Error fetching batch at offset {offset}: {e}")
            print(f"Response text: {response.text if 'response' in locals() else 'No response'}")
            break
    
    connection.close()
    logger.info(f"Finished fetch_all_igdb_games(). Total games fetched: {len(all_games)}")
    return all_games