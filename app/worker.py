"""
Affiliate Bot - Worker/Scheduler Entry Point
Runs scheduled jobs independently from API.
"""
import signal
import time
import schedule

from utils.logger import logger
from automation.scheduler import (
    daily_data_collection,
    check_and_run_monthly_igdb,
    post_single_tweet_job,
)
import config

shutdown_event = False

def signal_handler(signum, frame):
    """Handle shutdown gracefully"""
    global shutdown_event
    logger.info("Shutdown signal received")
    shutdown_event = True

def setup_scheduler():
    """Configure all scheduled jobs"""
    
    # Daily data collection at 2 AM
    schedule.every().day.at("02:00").do(daily_data_collection).tag("daily")
    
    # Monthly IGDB collection on 1st at 3 AM
    schedule.every().day.at("03:00").do(check_and_run_monthly_igdb).tag("monthly")

    # Independent tweet posting (single post per run)
    schedule.every(config.HOURS_BETWEEN_POSTS).hours.do(post_single_tweet_job).tag("twitter")
    
    logger.info("Scheduler configured:")
    logger.info("  - Daily data collection: 02:00 UTC")
    logger.info("  - Monthly IGDB collection: 03:00 UTC on 1st of month")
    logger.info(f"  - Tweet posting: every {config.HOURS_BETWEEN_POSTS} hours (independent job)")

def run_scheduler():
    """Run the scheduler loop"""
    global shutdown_event
    
    logger.info("="*60)
    logger.info("Affiliate Bot - Scheduler Started")
    logger.info("="*60)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    setup_scheduler()
    
    while not shutdown_event:
        try:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
        except Exception as e:
            logger.error(f"Scheduler error: {e}", exc_info=True)
            time.sleep(60)
    
    logger.info("Scheduler shutting down")

if __name__ == "__main__":
    run_scheduler()