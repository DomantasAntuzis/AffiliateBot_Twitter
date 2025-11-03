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
from services.affiliate_service import fetch_cj_products
from services.steam_service import fetch_steam_topsellers
from services.deals_service import find_matching_deals, scrape_indiegala_deals
from services.validation_service import validate_deals_batch
from services.twitter_service import post_deal_to_twitter

def daily_data_collection():
    """
    Daily job: Collect affiliate products, Steam data, find deals, and validate
    Runs once per day at scheduled time
    """
    logger.info("="*60)
    logger.info("Starting daily data collection and validation")
    logger.info("="*60)
    
    start_time = time.time()
    
    try:
        # Step 1: Fetch CJ Affiliate products
        logger.info("Step 1/5: Fetching CJ Affiliate products...")
        if not fetch_cj_products():
            logger.error("Failed to fetch CJ products")
            return
        
        # Step 2: Fetch Steam top sellers
        logger.info("Step 2/5: Fetching Steam top sellers...")
        if not fetch_steam_topsellers():
            logger.error("Failed to fetch Steam top sellers")
            return
        
        # Step 3: Find matching deals
        logger.info("Step 3/5: Finding matching deals...")
        deals = find_matching_deals()
        if not deals:
            logger.warning("No matching deals found")
            return
        
        # Step 4: Validate deals
        logger.info("Step 4/5: Validating deals...")
        valid_deals = validate_deals_batch(deals)
        if not valid_deals:
            logger.warning("No valid deals after validation")
            return
        
        # Step 5: Scrape IndieGala
        logger.info("Step 5/5: Scraping IndieGala...")
        scrape_indiegala_deals()
        
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
        logger.error(f"Error in daily data collection: {e}", exc_info=True)

def _shuffle_deals():
    """Shuffle deals within each source group for random posting"""
    try:
        # Load valid deals
        deals = load_json_file(config.VALID_DEALS_JSON)
        
        # Shuffle each group
        for group in deals:
            random.shuffle(group)
        
        # Save shuffled deals
        save_json_file(config.SHUFFLED_DEALS_JSON, deals)
        
        logger.info("Deals shuffled and saved")
        
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

def setup_scheduler():
    """
    Setup and configure the scheduler
    
    Returns:
        None
    """
    # Schedule daily job
    # For testing: run 2 minutes from now
    # For production: set to specific time like "00:00" for midnight
    
    now = datetime.datetime.now() + datetime.timedelta(minutes=2)
    run_time = now.strftime("%H:%M")
    
    schedule.every().day.at(run_time).do(daily_data_collection)
    
    logger.info("="*60)
    logger.info("🚀 Scheduler initialized!")
    logger.info(f"📅 Daily job scheduled for {run_time}")
    logger.info(f"⏰ Next run: {schedule.next_run()}")
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

