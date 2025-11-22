"""
Helper utility functions used across the application
"""
import os
import json
import config

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
    """
    Load and return JSON data from file
    
    Args:
        filepath: Path to JSON file
    
    Returns:
        dict/list: Parsed JSON data
    """
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        return []
    except json.JSONDecodeError:
        return []

def save_json_file(filepath, data, indent=4):
    """
    Save data to JSON file
    
    Args:
        filepath: Path to save JSON file
        data: Data to save
        indent: JSON indentation (default: 4)
    """
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=indent, ensure_ascii=False)

def load_posted_games(filepath="posted_games.txt"):
    """
    Load list of posted game titles from file
    
    Args:
        filepath: Path to posted games file
    
    Returns:
        list: List of posted game titles
    """
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return f.read().splitlines()
    except FileNotFoundError:
        return []

def save_posted_games(titles, filepath="posted_games.txt", limit=60):
    """
    Save posted game titles to file, maintaining rolling limit
    
    Args:
        titles: List of game titles
        filepath: Path to posted games file
        limit: Maximum number of titles to keep (default: 60)
    """
    # Keep only the last N titles
    if len(titles) > limit:
        titles = titles[-limit:]
    
    with open(filepath, 'w', encoding='utf-8') as f:
        for title in titles:
            f.write(f"{title}\n")

def add_posted_game(title, filepath="posted_games.txt", limit=60):
    """
    Add a new posted game title to the file
    
    Args:
        title: Game title to add
        filepath: Path to posted games file
        limit: Maximum number of titles to keep (default: 60)
    """
    if filepath is None:
        filepath = config.POSTED_GAMES_FILE
    posted_titles = load_posted_games(filepath)
    posted_titles.append(title)
    save_posted_games(posted_titles, filepath, limit)

