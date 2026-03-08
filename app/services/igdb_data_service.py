from requests import post
import os
import time
import sys

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.db_session import get_session
from database.queries.items import (
    get_genre_ids,
    get_distinct_item_ids_with_genres,
    batch_upsert_genres,
    batch_upsert_items,
    get_items_by_igdb_ids,
    batch_upsert_item_genres,
    get_items_with_igdb_cover_for_download,
)
from utils.logger import logger
from services.image_cache_service import download_igdb_image, is_image_cached
from utils.helpers import _normalize_title


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

        genre_tuples = [(g['id'], g['name']) for g in genres_batch]
        batch_upsert_genres(genre_tuples)
        batch_count += 1

        offset += limit
        if len(genres_batch) < limit:
            break

        time.sleep(0.3)

    logger.info("Finished fetch_all_genres()")
    return None

def fetch_all_igdb_games():
    """
    Fetch all games from IGDB API.
    Params: game_type = 0 (base game), platforms = 6 (PC)
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

    with get_session() as session:
        valid_genre_ids = get_genre_ids(session)
        items_with_genres = get_distinct_item_ids_with_genres(session)

        while True:
            query = (
                "fields name,genres,external_games,platforms,cover.image_id;"
                f"where game_type = 0 & release_dates.platform = 6;"
                f"limit {limit};"
                f"offset {offset};"
            )

            try:
                response = post('https://api.igdb.com/v4/games', headers=headers, data=query)

                if response.status_code != 200:
                    print(f"Error {response.status_code}: {response.text}")
                    if response.status_code == 429:
                        time.sleep(1)
                        continue
                    break

                games_batch = response.json()
                if not games_batch:
                    break

                items_to_insert = []
                igdb_ids_to_query = []

                for game in games_batch:
                    cover_image_id = None
                    if 'cover' in game and game['cover']:
                        if isinstance(game['cover'], dict) and 'image_id' in game['cover']:
                            cover_image_id = game['cover']['image_id']
                        elif isinstance(game['cover'], str):
                            cover_image_id = game['cover']
                    # normalized_title = _normalize_title(game['name'])
                    items_to_insert.append((game['name'], "base game", game["id"], cover_image_id))
                    igdb_ids_to_query.append(game["id"])

                try:
                    batch_upsert_items(items_to_insert, session=session)
                    print(f"Batch inserted {len(items_to_insert)} games")

                    igdb_to_item_map = get_items_by_igdb_ids(igdb_ids_to_query, session=session)

                    genre_inserts = []
                    for game in games_batch:
                        igdb_id = game["id"]
                        item_id = igdb_to_item_map.get(igdb_id)

                        if not item_id:
                            print(f"Warning: Could not find item_id for game {game.get('name', 'unknown')}")
                            continue

                        if item_id not in items_with_genres:
                            game_genres = game.get("genres", [])
                            if game_genres:
                                for genre_id in game_genres:
                                    if genre_id in valid_genre_ids:
                                        genre_inserts.append((item_id, genre_id))
                                    else:
                                        print(f"Warning: Genre ID {genre_id} not found for game {game.get('name', 'unknown')}")
                            items_with_genres.add(item_id)

                    if genre_inserts:
                        batch_upsert_item_genres(genre_inserts, session=session)
                        print(f"Batch inserted {len(genre_inserts)} genre relationships")

                    session.commit()
                    print(f"Committed batch {batch_count + 1}")

                except Exception as e:
                    print(f"ERROR processing batch: {e}")
                    logger.error(f"ERROR processing batch: {e}")
                    import traceback
                    traceback.print_exc()
                    session.rollback()

                all_games.extend(games_batch)
                batch_count += 1
                print(f"Batch {batch_count}: Total games collected: {len(all_games)}")
                print("-" * 60)

                time.sleep(0.3)

                if len(games_batch) < limit:
                    break

                offset += limit

            except Exception as e:
                print(f"Error fetching batch at offset {offset}: {e}")
                print(f"Response text: {response.text if 'response' in locals() else 'No response'}")
                break

    logger.info(f"Finished fetch_all_igdb_games(). Total games fetched: {len(all_games)}")
    return all_games

def download_igdb_images_for_items():
    """
    Download IGDB cover images for all items that have igdb_cover_image_id but images not yet downloaded.
    This can be run separately to download images in the background.
    """
    logger.info("Starting download_igdb_images_for_items()")

    try:
        items = get_items_with_igdb_cover_for_download()
        total_items = len(items)
        downloaded_count = 0
        skipped_count = 0
        failed_count = 0

        logger.info(f"Found {total_items} items with IGDB cover image IDs")

        if total_items == 0:
            logger.warning("No items found with IGDB cover image IDs. Make sure fetch_all_igdb_games() has been run first.")
            return False

        for row in items:
            item_id = row['id']
            image_id = row['igdb_cover_image_id']
            
            # Skip if image_id is invalid (0, empty, or None)
            if not image_id or str(image_id).strip() == '0' or str(image_id).strip() == '':
                skipped_count += 1
                continue
            
            # Convert to string and strip whitespace
            image_id = str(image_id).strip()
            
            # Skip if already cached
            if is_image_cached(image_id):
                skipped_count += 1
                if (skipped_count + downloaded_count) % 100 == 0:
                    logger.info(f"Processed {skipped_count + downloaded_count}/{total_items} items... ({skipped_count} cached, {downloaded_count} downloaded, {failed_count} failed)")
                continue
            
            # Download image
            try:
                result = download_igdb_image(image_id)
                if result:
                    downloaded_count += 1
                    if downloaded_count % 50 == 0:
                        logger.info(f"Downloaded {downloaded_count}/{total_items} images... ({skipped_count} cached, {failed_count} failed)")
                else:
                    failed_count += 1
                    if failed_count <= 10:  # Log first 10 failures for debugging
                        logger.warning(f"Failed to download image {image_id} for item {item_id}")
            except Exception as e:
                failed_count += 1
                logger.error(f"Exception downloading image {image_id} for item {item_id}: {e}")
            
            # Small delay to avoid rate limiting
            time.sleep(0.1)
        
        logger.info(f"Finished downloading images: {downloaded_count} downloaded, {skipped_count} already cached, {failed_count} failed")
        return True

    except Exception as e:
        logger.error(f"Error downloading IGDB images: {e}")
        import traceback
        traceback.print_exc()
        return False

def fetch_games_and_download_images():
    """
    Complete workflow: Fetch games from IGDB API (including cover image IDs) 
    and then download all images.
    Use this if you need to complete a partial data collection.
    """
    logger.info("="*60)
    logger.info("Starting complete IGDB data collection and image download")
    logger.info("="*60)
    
    # Step 1: Fetch games from IGDB API (this will update igdb_cover_image_id for all games)
    logger.info("Step 1/2: Fetching games from IGDB API...")
    fetch_all_genres()
    fetch_all_igdb_games()
    
    # Step 2: Download images for all items that now have cover image IDs
    logger.info("Step 2/2: Downloading images...")
    download_igdb_images_for_items()
    
    logger.info("="*60)
    logger.info("Complete workflow finished!")
    logger.info("="*60)

if __name__ == "__main__":
    # Allow manual execution for testing
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "download-images":
        download_igdb_images_for_items()
    elif len(sys.argv) > 1 and sys.argv[1] == "complete":
        fetch_games_and_download_images()
    else:
        # Run normal data collection
        fetch_all_genres()
        fetch_all_igdb_games()