"""
Automated task scheduler
Manages all scheduled jobs for the affiliate bot
"""

import datetime
import random
import time
from database.queries.twitter_posts import get_recent_posted_titles, get_today_post_count
import config
from services.affiliate_service import fetch_all_affiliate_products
from services.deals_service import find_matching_deals
from services.igdb_data_service import (
    download_igdb_images_for_items,
    fetch_all_genres,
    fetch_all_igdb_games,
)
from services.twitter_service import post_deal_to_twitter
from utils.helpers import save_json_file, load_json_file
from utils.logger import logger
from services.steam_service import fetch_and_save_steam_topsellers


def daily_data_collection():
    """
    Daily job: Collect affiliate products, Steam data, find deals, and validate
    Runs once per day at scheduled time
    """
    logger.info("=" * 60)
    logger.info("Starting daily data collection and validation")
    logger.info("=" * 60)

    try:
        start_time = time.time()
        logger.info("Step 1/4: Fetching all affiliate products (CJ + IndieGala + GamersGate)...")
        success, products_list = fetch_all_affiliate_products()
        if not success:
            logger.error("Failed to fetch affiliate products")
            return

        logger.info("Step 2/4: Fetching and saving top 500 Steam sellers to database...")
        if not fetch_and_save_steam_topsellers():
            logger.error("Failed to fetch and save Steam top sellers")
            return

        logger.info("Step 3/4: Finding matching deals...")
        deals = find_matching_deals(products_list=products_list)
        if not deals:
            logger.warning("No matching deals found")
            return

        logger.info("Step 4/4: Saving deal pool for independent posting job...")
        save_json_file(config.SHUFFLED_DEALS_JSON, deals)

        end_time = time.time()
        elapsed_time = end_time - start_time

        logger.info("=" * 60)
        logger.info(
            f"Data collection and validation completed in {elapsed_time:.2f} seconds"
        )
        logger.info("=" * 60)

        logger.info("Daily collection finished. Posting is handled by independent scheduled job.")

    except Exception as e:
        logger.error(f"Error in daily data collection: {e}", exc_info=True)


def post_single_tweet_job():
    """
    Independent posting job (single attempt per run).
    This job is scheduled separately from daily collection, so crashes/restarts
    do not block data collection and vice versa.
    """
    logger.info("Running independent tweet posting job")

    try:
        today_posts = get_today_post_count()
        if today_posts >= config.POSTS_PER_DAY:
            logger.info(
                f"Daily post limit reached ({today_posts}/{config.POSTS_PER_DAY}); skipping this run"
            )
            return

        deals_list = load_json_file(config.SHUFFLED_DEALS_JSON)
        if not deals_list:
            logger.info("No cached deals found for posting; rebuilding deal pool")
            deals_list = find_matching_deals()
            if not deals_list:
                logger.warning("Could not build deal pool for posting")
                return
            save_json_file(config.SHUFFLED_DEALS_JSON, deals_list)

        posted_games_list = get_recent_posted_titles()
        deal = select_unposted_deal(deals_list, posted_games_list)
        if not deal:
            logger.warning("No eligible deal available to post right now")
            save_json_file(config.SHUFFLED_DEALS_JSON, deals_list)
            return

        if post_deal_to_twitter(deal):
            save_json_file(config.SHUFFLED_DEALS_JSON, deals_list)
            logger.info("Independent tweet posting job posted 1 tweet successfully")
        else:
            logger.error("Independent tweet posting job failed to post tweet")

    except Exception as e:
        logger.error(f"Error in independent tweet posting job: {e}", exc_info=True)


def select_unposted_deal(deals, posted_games_list):
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
        if (
            deal["title"] in posted_games_list
            or int(deal.get("discount") or 0) <= config.MIN_DISCOUNT_THRESHOLD
        ):
            deals[rng].pop(0)
            # Remove empty group
            if not deals[rng]:
                del deals[rng]
            attempts += 1
            continue

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
    logger.info("=" * 60)
    logger.info("Starting monthly IGDB data collection")
    logger.info("=" * 60)

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

        logger.info("=" * 60)
        logger.info(
            f"Monthly IGDB data collection completed in {elapsed_time:.2f} seconds"
        )
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Error in monthly IGDB data collection: {e}")
        import traceback

        traceback.print_exc()
