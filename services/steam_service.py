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
from database.db_connect import get_connection, execute_many

def fetch_steam_topsellers():
    """
    Fetch Steam top sellers and save to database and CSV
    
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
    
    # Save to database (primary storage)
    db_success = _save_to_database(topsellers)
    
    # Save to CSV (backup)
    csv_success = _save_to_csv(topsellers)
    
    if db_success:
        logger.info(f"Successfully fetched and saved {len(topsellers)} Steam top sellers to database")
    
    return db_success

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
            
            if len(batch) < 100:
                logger.warning(f"Only {len(batch)} games returned, likely reached end of results")
                break
                
        except Exception as e:
            logger.error(f"Error fetching batch {i+1}: {e}")
            break
    
    logger.info(f"Total fetched: {len(all_results)} games")
    return all_results

def _parse_price(price_str):
    """
    Parse price string to decimal value
    
    Args:
        price_str: Price string (e.g., "$29.99", "Free", "N/A", "29.99")
    
    Returns:
        float: Parsed price value, or 0.00 if price is Free/N/A/invalid
    """
    if not price_str or price_str in ["Free", "N/A", "?", ""]:
        return 0.00
    
    try:
        # Remove currency symbols, commas, and whitespace
        cleaned = str(price_str).replace("$", "").replace("€", "").replace("£", "").replace(",", "").strip()
        
        # Try to extract numeric value (handle cases like "$29.99" or "29.99")
        import re
        match = re.search(r'(\d+\.?\d*)', cleaned)
        if match:
            return float(match.group(1))
        
        return 0.00
    except Exception:
        return 0.00

def _save_to_database(games):
    """
    Save games to topsellers database table
    First empties the table, then inserts new data with ranking as ID
    
    Args:
        games: List of game dictionaries (ordered by ranking)
    
    Returns:
        bool: True if successful, False otherwise
    """
    import re
    
    def _clean_title(title):
        """Remove emojis and special characters that might cause encoding issues, and truncate if too long"""
        # Remove emojis (4-byte UTF-8 characters)
        emoji_pattern = re.compile(
            "["
            "\U0001F600-\U0001F64F"  # emoticons
            "\U0001F300-\U0001F5FF"  # symbols & pictographs
            "\U0001F680-\U0001F6FF"  # transport & map symbols
            "\U0001F1E0-\U0001F1FF"  # flags (iOS)
            "\U00002702-\U000027B0"
            "\U000024C2-\U0001F251"
            "\U0001F900-\U0001F9FF"  # Supplemental Symbols and Pictographs
            "\U0001FA00-\U0001FA6F"  # Chess Symbols
            "\U0001FA70-\U0001FAFF"  # Symbols and Pictographs Extended-A
            "]+", 
            flags=re.UNICODE
        )
        cleaned = emoji_pattern.sub('', title)
        # Remove any remaining 4-byte UTF-8 characters
        cleaned = cleaned.encode('utf-8', 'ignore').decode('utf-8')
        cleaned = cleaned.strip()
        
        # Truncate to 255 characters (database column limit)
        if len(cleaned) > 255:
            cleaned = cleaned[:255]
        
        return cleaned
    
    try:
        connection = get_connection()
        if not connection:
            logger.error("Failed to get database connection")
            return False
        
        cursor = connection.cursor()
        
        # Step 1: Empty the topsellers table (delete all rows)
        cursor.execute("DELETE FROM topsellers")
        logger.info("Cleared topsellers table")
        
        # Step 2: Prepare data for batch insert
        # ID represents ranking (1-500), title is the game name, price is decimal(10,2)
        # Skip games with "Free" prices since hidef2p=1 should prevent free games
        insert_values = []
        skipped_free = 0
        ranking = 1
        
        for game in games:
            cleaned_title = _clean_title(game['title'])
            if not cleaned_title:
                continue
            
            price_str = game.get('price', 'N/A')
            # Skip free games (shouldn't appear with hidef2p=1, but handle edge cases)
            if price_str in ["Free", "N/A", "?", ""]:
                skipped_free += 1
                logger.warning(f"Skipping game with invalid/free price: '{cleaned_title}' (price: '{price_str}')")
                continue
            
            # Parse price from string (e.g., "$29.99" -> 29.99)
            price_value = _parse_price(price_str)
            
            # Double-check: if parsed price is 0.00, skip it (shouldn't happen with hidef2p)
            if price_value == 0.00:
                skipped_free += 1
                logger.warning(f"Skipping game with 0.00 price: '{cleaned_title}' (original: '{price_str}')")
                continue
            
            insert_values.append((ranking, cleaned_title, price_value))
            ranking += 1
        
        # Step 3: Batch insert new data
        insert_query = "INSERT INTO topsellers (id, title, price) VALUES (%s, %s, %s)"
        execute_many(insert_query, insert_values, connection=connection)
        
        connection.commit()
        cursor.close()
        connection.close()
        
        if skipped_free > 0:
            logger.warning(f"Skipped {skipped_free} games with free/invalid prices (should not occur with hidef2p=1)")
        
        logger.info(f"Successfully inserted {len(insert_values)} top sellers into database")
        return True
        
    except Exception as e:
        logger.error(f"Error saving to database: {e}")
        if connection:
            try:
                connection.rollback()
                connection.close()
            except:
                pass
        return False

def _save_to_csv(games):
    """
    Save games to CSV file (backup storage)
    
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
    Read and return Steam games from database
    
    Returns:
        list: List of [title, price] items (ordered by ranking)
    """
    try:
        connection = get_connection()
        if not connection:
            logger.error("Failed to get database connection")
            return []
        
        cursor = connection.cursor()
        cursor.execute("SELECT title, price FROM topsellers ORDER BY id ASC")
        rows = cursor.fetchall()
        cursor.close()
        connection.close()
        
        # Convert to list format: [[title, price], [title, price], ...]
        games = [[row[0], row[1]] for row in rows]
        
        logger.info(f"Loaded {len(games)} Steam games from database")
        return games
        
    except Exception as e:
        logger.error(f"Error reading Steam data from database: {e}")
        return []

