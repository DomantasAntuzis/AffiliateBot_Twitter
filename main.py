"""
Affiliate Bot - Main Entry Point
Automated affiliate marketing bot that finds and posts game deals to Twitter
"""
import sys
import os

# Ensure config and utils are importable
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils.logger import logger
from utils.helpers import ensure_directories
from automation.scheduler import run_scheduler

def main():
    """
    Main entry point for the affiliate bot
    """
    logger.info("="*60)
    logger.info("Starting Affiliate Bot")
    logger.info("="*60)
    
    # Ensure all required directories exist
    ensure_directories()
    logger.info("Directories verified")
    
    # Start the scheduler (runs indefinitely)
    try:
        run_scheduler()
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
