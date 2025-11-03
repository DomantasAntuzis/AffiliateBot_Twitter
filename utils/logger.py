"""
Centralized logging configuration for Affiliate Bot
"""
import logging
import os
from datetime import datetime

def setup_logger(name="AffiliateBot", log_level=logging.INFO):
    """
    Configure and return a logger instance
    
    Args:
        name: Logger name
        log_level: Logging level (default: INFO)
    
    Returns:
        logger: Configured logger instance
    """
    # Create logs directory if it doesn't exist
    os.makedirs("logs", exist_ok=True)
    
    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(log_level)
    
    # Prevent duplicate handlers
    if logger.handlers:
        return logger
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # File handler - writes to app.log
    file_handler = logging.FileHandler('logs/app.log', encoding='utf-8')
    file_handler.setLevel(log_level)
    file_handler.setFormatter(formatter)
    
    # Console handler - writes to stdout
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)
    
    # Add handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

# Create default logger instance for import
logger = setup_logger()

