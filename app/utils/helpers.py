"""
Helper utility functions used across the application
"""

import json
import os
import re

import config
from utils.logger import logger

from database.queries.twitter_posts import get_recent_posted_titles
from database.queries.steam import get_steam_topsellers


def ensure_directories():
    """Create all necessary directories if they don't exist"""
    directories = [
        config.DATA_DIR,
        config.CSV_DIR,
        config.JSON_DIR,
        config.IMAGES_DIR,
        config.LOGS_DIR,
        config.TEMP_DIR
    ]
    for directory in directories:
        os.makedirs(os.path.join(config.SERVER_DIR, directory), exist_ok=True)


def load_json_file(filepath):
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return []
    except json.JSONDecodeError:
        return []


def save_json_file(filepath, data, indent=4):
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=indent, ensure_ascii=False)

def _get_steam_topsellers():
    """Load Steam top sellers from database. Returns [[title, price], ...]."""
    try:
        return get_steam_topsellers()
    except Exception as e:
        logger.error(f"Error loading Steam prices from database: {e}")
        return []


def _normalize_title(title):
    """
    Normalize title for fuzzy matching by removing punctuation and normalizing whitespace
    """
    # Convert to lowercase
    normalized = title.lower().strip()
    
    # Replace common punctuation with spaces (hyphens, colons, semicolons, etc.)
    normalized = re.sub(r'[-:;–—]', ' ', normalized)
    
    # Remove other punctuation (keep apostrophes for names like "O'Brien")
    normalized = re.sub(r'[^\w\s\']', '', normalized)
    
    # Collapse multiple spaces to single space
    normalized = re.sub(r'\s+', ' ', normalized)
    
    # Trim
    normalized = normalized.strip()
    
    return normalized