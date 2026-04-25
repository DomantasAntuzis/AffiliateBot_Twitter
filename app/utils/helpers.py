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


def normalize_title(title):
    normalized = title.lower().strip()
    normalized = re.sub(r'[-:;–—]', ' ', normalized)
    normalized = re.sub(r'[^\w\s\']', '', normalized)
    normalized = re.sub(r'\s+', ' ', normalized)
    normalized = normalized.strip()
    
    return normalized


def normalize_match_title(title: str) -> str:
    """Normalization used for cross-source title matching and persisted norm_title fields."""
    normalized = normalize_title(title or "")
    normalized = normalized.replace("'", "")

    roman_map = {
        "x": "10",
        "ix": "9",
        "viii": "8",
        "vii": "7",
        "vi": "6",
        "v": "5",
        "iv": "4",
        "iii": "3",
        "ii": "2",
        "i": "1",
    }
    for roman, arabic in roman_map.items():
        normalized = re.sub(rf"\b{roman}\b", arabic, normalized)

    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized[:255]

def normalize_distributor_name(program_name):
  program_name = program_name.strip()
  
  # Mapping from CSV PROGRAM_NAME to database name
  name_mapping = {
  "GamersGate.com": "GamersGate",
  "GOG.COM INT": "GOG",
  "YUPLAY": "YUPLAY",  # Database has YUPLAY in all caps
  "IndieGala": "IndieGala",
  }
  
  return name_mapping.get(program_name, program_name)