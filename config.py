"""
Configuration management for Affiliate Bot
Centralized place for all configuration settings and environment variables
"""
import os

SERVER_DIR = os.path.dirname(os.path.abspath(__file__))

# CJ Affiliate API credentials
CJ_HTTP_USERNAME = os.getenv("CJ_HTTP_USERNAME")
CJ_HTTP_PASSWORD = os.getenv("CJ_HTTP_PASSWORD")

# Twitter API credentials
TWITTER_BEARER_TOKEN = os.getenv("TWITTER_BEARER_TOKEN")
TWITTER_API_KEY = os.getenv("TWITTER_API_KEY")
TWITTER_API_KEY_SECRET = os.getenv("TWITTER_API_KEY_SECRET")
TWITTER_ACCESS_TOKEN = os.getenv("TWITTER_ACCESS_TOKEN")
TWITTER_ACCESS_TOKEN_SECRET = os.getenv("TWITTER_ACCESS_TOKEN_SECRET")

# Proxy configuration
ROTATING_PROXY = "http://p.webshare.io:9999"

# Database configuration
DB_HOST = "localhost"
DB_USER = "root"
DB_PASSWORD = ""
DB_NAME = "affiliate_marketing"

# File paths
DATA_DIR = os.path.join(SERVER_DIR, "data")
CSV_DIR = os.path.join(DATA_DIR, "csv")
JSON_DIR = os.path.join(DATA_DIR, "json")
IMAGES_DIR = os.path.join(DATA_DIR, "images")
LOGS_DIR = os.path.join(SERVER_DIR, "logs")
TEMP_DIR = os.path.join(SERVER_DIR, "items_files")

# CSV files
PRODUCTS_CSV = os.path.join(CSV_DIR, "items_info.csv")
STEAMDB_CSV = os.path.join(CSV_DIR, "steamdb_results.csv")
MISSING_TITLES_CSV = os.path.join(CSV_DIR, "missing_game_titles.csv")

# JSON files
VALID_DEALS_JSON = os.path.join(JSON_DIR, "valid_deals.json")
SHUFFLED_DEALS_JSON = os.path.join(JSON_DIR, "shuffled_deals.json")

# Text files
POSTED_GAMES_FILE = os.path.join(SERVER_DIR, "posted_games.txt")

# Application settings
POSTED_GAMES_LIMIT = 60
MIN_DISCOUNT_THRESHOLD = 10
STEAM_TOP_SELLERS_COUNT = 500
POSTS_PER_DAY = 6
HOURS_BETWEEN_POSTS = 4

# Steam settings
STEAM_REGION = "US"
STEAM_LANGUAGE = "en"

# Browser pool settings
BROWSER_POOL_SIZE = 3
VALIDATION_WORKERS = 3

