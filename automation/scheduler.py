"""
Automated task scheduler
Manages all scheduled jobs for the affiliate bot
"""
import schedule
import time
import random
import json
import datetime
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import config
from utils.logger import logger
from utils.helpers import load_json_file, save_json_file, load_posted_games
from services.affiliate_service import fetch_all_affiliate_products, insert_gamersgate_offers
from services.steam_service import fetch_steam_topsellers
from services.deals_service import find_matching_deals
from services.validation_service import validate_deals_batch
from services.twitter_service import post_deal_to_twitter
from services.igdb_data_service import fetch_all_genres, fetch_all_igdb_games, download_igdb_images_for_items

def daily_data_collection():
    """
    Daily job: Collect affiliate products, Steam data, find deals, and validate
    Runs once per day at scheduled time
    """
    logger.info("="*60)
    logger.info("Starting daily data collection and validation")
    logger.info("="*60)
    
    try:
        # Step 1: Fetch all affiliate products (CJ + IndieGala)
        start_time = time.time()
        logger.info("Step 1/4: Fetching all affiliate products (CJ + IndieGala)...")
        if not fetch_all_affiliate_products():
            logger.error("Failed to fetch affiliate products")
            return
        
        # Step 1b: Insert GamersGate offers (matches with affiliate products from CSV)
        logger.info("Step 1b/4: Inserting GamersGate offers...")
        insert_gamersgate_offers()
        
        # Step 2: Fetch Steam top sellers
        logger.info("Step 2/4: Fetching Steam top sellers...")
        if not fetch_steam_topsellers():
            logger.error("Failed to fetch Steam top sellers")
            return
        
        # Step 3: Find matching deals
        logger.info("Step 3/4: Finding matching deals...")
        deals = find_matching_deals()
        if not deals:
            logger.warning("No matching deals found")
            return
        
        # Step 4: Validate deals
        logger.info("Step 4/4: Validating deals...")
        valid_deals = validate_deals_batch(deals)
        if not valid_deals:
            logger.warning("No valid deals after validation")
            return
        
        end_time = time.time()
        elapsed_time = end_time - start_time
        
        logger.info("="*60)
        logger.info(f"Data collection and validation completed in {elapsed_time:.2f} seconds")
        logger.info("="*60)
        
        # Shuffle deals for random posting
        _shuffle_deals()
        
        # Post first batch of tweets
        _post_tweets_batch()
        
    except Exception as e:
        logger.error(f"Error in daily data collection: {e}")

def _shuffle_deals():
    """Shuffle deals within each source group for random posting"""
    try:
        # Load valid deals
        deals = load_json_file(config.VALID_DEALS_JSON)
        
        # Load posted games to filter them out
        posted_games_list = load_posted_games(config.POSTED_GAMES_FILE)
        
        # Filter out already-posted games from each group
        filtered_deals = []
        for group in deals:
            filtered_group = [
                deal for deal in group 
                if deal["title"] not in posted_games_list
            ]
            if filtered_group:  # Only add non-empty groups
                filtered_deals.append(filtered_group)
        
        # Shuffle each remaining group
        for group in filtered_deals:
            random.shuffle(group)
        
        # Save shuffled deals (without posted games)
        save_json_file(config.SHUFFLED_DEALS_JSON, filtered_deals)
        
        logger.info(f"Deals shuffled and saved. Filtered out {len(posted_games_list)} already-posted games.")
        
    except Exception as e:
        logger.error(f"Error shuffling deals: {e}")
        
def _post_tweets_batch():
    """Post a batch of tweets (6 tweets, 4 hours apart)"""
    logger.info("Starting tweet posting batch")
    
    post_count = 0
    
    while post_count < config.POSTS_PER_DAY:
        try:
            # Load current deals
            with open(config.SHUFFLED_DEALS_JSON, 'r', encoding='utf-8') as f:
                deals = json.load(f)
            
            # Load posted games
            posted_games_list = load_posted_games(config.POSTED_GAMES_FILE)
            
            # Find a valid deal to post
            deal = _select_unposted_deal(deals, posted_games_list)
            
            if not deal:
                logger.warning("No more deals to post")
                break
            
            # Post tweet
            if post_deal_to_twitter(deal):
                post_count += 1
                logger.info(f"Posted {post_count}/{config.POSTS_PER_DAY} tweets")
                
                # Save updated deals (with posted item removed)
                save_json_file(config.SHUFFLED_DEALS_JSON, deals)
                
                # Wait before next post (except for last post)
                if post_count < config.POSTS_PER_DAY:
                    sleep_time = config.HOURS_BETWEEN_POSTS * 60 * 60
                    logger.info(f"Waiting {config.HOURS_BETWEEN_POSTS} hours before next post...")
                    time.sleep(sleep_time)
            else:
                logger.error("Failed to post tweet, continuing...")
                continue
                
        except Exception as e:
            logger.error(f"Error in tweet posting: {e}")
            continue
    
    logger.info(f"Tweet posting batch completed. Posted {post_count} tweets.")

def _select_unposted_deal(deals, posted_games_list):
    """
    Select a random unposted deal from the deals list
    
    Args:
        deals: List of deal groups
        posted_games_list: List of posted game titles
    
    Returns:
        dict: Selected deal or None if no deals available
    """
    max_attempts = 100
    attempts = 0
    
    while attempts < max_attempts:
        if not deals:
            logger.warning("No deals left to post")
            return None
        
        # Pick random group
        rng = random.randint(0, len(deals) - 1)
        
        # Skip empty groups
        if not deals[rng]:
            del deals[rng]
            continue
        
        # Get first deal from group
        deal = deals[rng][0]
        
        # Check if deal is valid
        if deal["title"] in posted_games_list or deal.get("discount", 0) <= config.MIN_DISCOUNT_THRESHOLD:
            deals[rng].pop(0)
            # Remove empty group
            if not deals[rng]:
                del deals[rng]
            attempts += 1
            continue
        else:
            # Found valid deal, remove it from list
            deals[rng].pop(0)
            if not deals[rng]:
                del deals[rng]
            return deal
    
    logger.warning(f"Could not find valid deal after {max_attempts} attempts")
    return None

def monthly_igdb_data_collection():
    """
    Monthly job: Fetch all genres and games from IGDB API, then download images
    Runs once per month at scheduled time
    """
    logger.info("="*60)
    logger.info("Starting monthly IGDB data collection")
    logger.info("="*60)
    
    start_time = time.time()
    
    try:
        # Step 1: Fetch all genres
        logger.info("Step 1/3: Fetching all genres from IGDB...")
        fetch_all_genres()
        logger.info("Genres fetch completed")
        
        # Step 2: Fetch all games (includes cover image IDs)
        logger.info("Step 2/3: Fetching all games from IGDB...")
        fetch_all_igdb_games()
        logger.info("Games fetch completed")
        
        # Step 3: Download images for games that don't have them yet
        logger.info("Step 3/3: Downloading images for games without cached images...")
        download_igdb_images_for_items()
        logger.info("Image download completed")
        
        end_time = time.time()
        elapsed_time = end_time - start_time
        
        logger.info("="*60)
        logger.info(f"Monthly IGDB data collection completed in {elapsed_time:.2f} seconds")
        logger.info("="*60)
        
    except Exception as e:
        logger.error(f"Error in monthly IGDB data collection: {e}")
        import traceback
        traceback.print_exc()

def _check_and_run_monthly_igdb():
    """
    Wrapper function to check if it's the first day of the month
    and run monthly IGDB data collection if needed
    """
    today = datetime.datetime.now()
    if today.day == 1:
        monthly_igdb_data_collection()

def setup_scheduler():
    """
    Setup and configure the scheduler
    
    Returns:
        None
    """
   
    now = datetime.datetime.now() + datetime.timedelta(minutes=2)
    run_time = now.strftime("%H:%M")
    
    schedule.every().day.at(run_time).do(daily_data_collection)
    
    schedule.every().day.at("02:00").do(_check_and_run_monthly_igdb).tag("monthly_igdb")
    
    logger.info("="*60)
    logger.info("Scheduler initialized!")
    logger.info(f"Daily job scheduled for {run_time}")
    logger.info("Monthly IGDB data collection scheduled for 1st of each month at 02:00")
    logger.info(f"Next daily run: {schedule.next_run()}")
    logger.info("="*60)

def run_scheduler():
    """
    Run the scheduler loop
    Checks for pending tasks every 60 seconds
    """
    setup_scheduler()
    
    while True:
        schedule.run_pending()
        time.sleep(60)

