"""
Steam API integration service
Handles fetching Steam top sellers data
"""
import requests
from bs4 import BeautifulSoup
import csv
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import config
from utils.logger import logger

def fetch_steam_topsellers():
    """
    Fetch Steam top sellers and save to CSV
    
    Returns:
        bool: True if successful, False otherwise
    """
    logger.info("Starting Steam top sellers fetch")
    
    topsellers = _fetch_top500_topsellers(
        cc=config.STEAM_REGION,
        lang=config.STEAM_LANGUAGE
    )
    
    if not topsellers:
        logger.error("Failed to fetch Steam top sellers")
        return False
    
    # Save to CSV
    success = _save_to_csv(topsellers)
    
    if success:
        logger.info(f"Successfully fetched {len(topsellers)} Steam top sellers")
    
    return success

def _fetch_batch(start, count=100, cc="US", lang="en"):
    """
    Fetch a batch of games from Steam starting at 'start' position
    
    Args:
        start: Starting position
        count: Number of games to fetch (default: 100)
        cc: Country code (default: US)
        lang: Language (default: en)
    
    Returns:
        list: List of game dictionaries
    """
    url = "https://store.steampowered.com/search/"
    params = {
        "filter": "globaltopsellers",
        "count": count,
        "start": start,
        "cc": cc,
        "l": lang,
        "category1": 998,      # Only games (not DLCs, software, etc.)
        "hidef2p": 1,          # Hide free-to-play games
        "infinite": 1,
        "force_infinite": 1
    }
    headers = {
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/125.0.0.0 Safari/537.36"),
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "X-Requested-With": "XMLHttpRequest",
        "Referer": "https://store.steampowered.com/search/"
    }
    
    try:
        r = requests.get(url, params=params, headers=headers, timeout=20)
        r.raise_for_status()
        data = r.json()
        
        if "results_html" not in data:
            return []
        
        html = data["results_html"]
        soup = BeautifulSoup(html, "html.parser")
        results = []
        
        for row in soup.select(".search_result_row"):
            appid = row.get("data-ds-appid") or row.get("data-ds-packageid") or "?"
            title = row.select_one(".title")
            if not title:
                continue
            
            title_text = title.get_text(strip=True)
            price_el = row.select_one(".discount_final_price")
            price = price_el.get_text(strip=True) if price_el else "N/A"
            discount_el = row.select_one(".discount_pct")
            discount = discount_el.get_text(strip=True) if discount_el else "0%"
            
            results.append({
                "appid": appid,
                "title": title_text,
                "price": price,
                "discount": discount
            })
        
        return results
        
    except Exception as e:
        logger.error(f"Error fetching Steam batch: {e}")
        return []

def _fetch_top500_topsellers(cc="US", lang="en"):
    """
    Fetch exactly 500 top sellers from Steam using multiple API calls
    
    Args:
        cc: Country code (default: US)
        lang: Language (default: en)
    
    Returns:
        list: List of game dictionaries
    """
    all_results = []
    
    # Steam API limits to 100 results per request, so we need 5 requests for 500 items
    for i in range(5):
        start_pos = i * 100
        logger.info(f"Fetching batch {i+1}/5 (items {start_pos+1}-{start_pos+100})...")
        
        try:
            batch = _fetch_batch(start_pos, 100, cc, lang)
            all_results.extend(batch)
            logger.info(f"Got {len(batch)} games")
            
            if len(batch) < 100:
                logger.warning(f"Only {len(batch)} games returned, likely reached end of results")
                break
                
        except Exception as e:
            logger.error(f"Error fetching batch {i+1}: {e}")
            break
    
    logger.info(f"Total fetched: {len(all_results)} games")
    return all_results

def _save_to_csv(games):
    """
    Save games to CSV file
    
    Args:
        games: List of game dictionaries
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Ensure CSV directory exists
        os.makedirs(config.CSV_DIR, exist_ok=True)
        
        with open(config.STEAMDB_CSV, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["name", "price"])
            
            for game in games:
                writer.writerow([game["title"], game["price"]])
        
        logger.info(f"Results saved to {config.STEAMDB_CSV}")
        return True
        
    except Exception as e:
        logger.error(f"Error saving to CSV: {e}")
        return False

def get_steam_prices():
    """
    Read and return Steam games with prices from CSV
    
    Returns:
        list: List of [title, price] pairs
    """
    try:
        with open(config.STEAMDB_CSV, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            games = list(reader)
        
        logger.info(f"Loaded {len(games)-1} Steam games from CSV")  # -1 for header
        return games
        
    except FileNotFoundError:
        logger.error(f"Steam CSV not found: {config.STEAMDB_CSV}")
        return []
    except Exception as e:
        logger.error(f"Error reading Steam CSV: {e}")
        return []

