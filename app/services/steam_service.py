"""
Steam API integration service
Handles fetching Steam top sellers data
"""
import requests
from bs4 import BeautifulSoup
import re

import config
from utils.logger import logger
from database.queries.steam import insert_topsellers

def fetch_batch(start, count=100, cc="US", lang="en"):
    proxy_url = config.PROXY_URL
    session = requests.Session()
    session.proxies = {"http": proxy_url, "https": proxy_url}
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
        r = session.get(url, params=params, headers=headers, timeout=20)
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

def fetch_top500_topsellers(cc="US", lang="en"):
    all_results = []
    
    # Steam API limits to 100 results per request, so we need 5 requests for 500 items
    for i in range(5):
        start_pos = i * 100
        logger.info(f"Fetching batch {i+1}/5 (items {start_pos+1}-{start_pos+100})...")
        
        try:
            batch = fetch_batch(start_pos, 100, cc, lang)
            all_results.extend(batch)
            
            if len(batch) < 100:
                logger.warning(f"Only {len(batch)} games returned, likely reached end of results")
                break
                
        except Exception as e:
            logger.error(f"Error fetching batch {i+1}: {e}")
            break
    
    logger.info(f"Total fetched: {len(all_results)} games")
    return all_results

def parse_price(price_str):
    if not price_str or price_str in ["Free", "N/A", "?", ""]:
        return 0.00
    
    try:
        # Remove currency symbols, commas, and whitespace
        cleaned = str(price_str).replace("$", "").replace("€", "").replace("£", "").replace(",", "").strip()
        
        # Try to extract numeric value (handle cases like "$29.99" or "29.99")
        match = re.search(r'(\d+\.?\d*)', cleaned)
        if match:
            return float(match.group(1))
        
        return 0.00
    except Exception:
        return 0.00

def clean_title(title):
    
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


def save_to_database(games):
    try:
        insert_values = []
        skipped_free = 0
        ranking = 1

        for game in games:
            cleaned_title = clean_title(game['title'])
            if not cleaned_title:
                continue

            price_str = game.get('price', 'N/A')
            if price_str in ["Free", "N/A", "?", ""]:
                skipped_free += 1
                logger.warning(f"Skipping game with invalid/free price: '{cleaned_title}' (price: '{price_str}')")
                continue

            price_value = parse_price(price_str)
            if price_value == 0.00:
                skipped_free += 1
                logger.warning(f"Skipping game with 0.00 price: '{cleaned_title}' (original: '{price_str}')")
                continue

            insert_values.append((ranking, cleaned_title, price_value))
            ranking += 1

        if not insert_values:
            logger.error("No valid games to insert, keeping existing topsellers data")
            return False

        count = insert_topsellers(insert_values)
        if skipped_free > 0:
            logger.warning(f"Skipped {skipped_free} games with free/invalid prices")
        logger.info(f"Successfully inserted {count} top sellers into database")
        return True

    except Exception as e:
        logger.error(f"Error saving to database: {e}")
        return False


def fetch_and_save_steam_topsellers(cc=None, lang=None):
    cc = cc or config.STEAM_REGION
    lang = lang or config.STEAM_LANGUAGE
    logger.info("Starting Steam top sellers fetch and save")
    games = fetch_top500_topsellers(cc=cc, lang=lang)
    if not games:
        logger.error("Failed to fetch Steam top sellers")
        return False
    success = save_to_database(games)
    if success:
        logger.info("Steam top sellers fetch and save completed successfully")
    return success
