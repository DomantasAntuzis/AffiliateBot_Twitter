"""
Deals matching and scraping service
Matches affiliate products with Steam top sellers and scrapes additional sources
"""
import csv
import json
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import config
from utils.logger import logger
from utils.helpers import load_posted_games, load_json_file, save_json_file

def find_matching_deals():
    """
    Find deals by matching affiliate products with Steam top sellers
    
    Returns:
        list: List of matching deals
    """
    logger.info("Finding deals by matching affiliate products with Steam top sellers")
    
    try:
        # Read product file
        with open(config.PRODUCTS_CSV, encoding="utf-8") as pf:
            product_reader = csv.reader(pf)
            product_list = list(product_reader)
        
        # Read Steam file
        with open(config.STEAMDB_CSV, encoding="utf-8") as sf:
            steam_reader = csv.reader(sf)
            steam_list = list(steam_reader)
        
        deals = []
        
        # Match products with Steam top sellers
        for s_row in steam_list:
            target_title = s_row[0]
            for p_row in product_list:
                if target_title == p_row[2] and (p_row[5] == "in stock" or p_row[5] == "in_stock"):
                    game_obj = {
                        "source": p_row[0],
                        "title": p_row[2],
                        "link": p_row[3],
                        "image_link": p_row[4],
                    }
                    deals.append(game_obj)
        
        logger.info(f"Found {len(deals)} matching deals")
        return deals
        
    except FileNotFoundError as e:
        logger.error(f"File not found: {e}")
        return []
    except Exception as e:
        logger.error(f"Error finding deals: {e}")
        return []

def scrape_indiegala_deals():
    """
    Scrape deals from IndieGala website
    
    Returns:
        bool: True if successful, False otherwise
    """
    logger.info("Starting IndieGala scraping")
    
    url = "https://www.indiegala.com/store/games/on-sale"
    
    chrome_options = Options()
    prefs = {
        'profile.default_content_setting_values': {
            'cookies': 2, 'images': 2, 'plugins': 2, 'popups': 2, 'geolocation': 2,
            'notifications': 2, 'auto_select_certificate': 2, 'fullscreen': 2,
            'mouselock': 2, 'mixed_script': 2, 'media_stream': 2,
            'media_stream_mic': 2, 'media_stream_camera': 2, 'protocol_handlers': 2,
            'ppapi_broker': 2, 'automatic_downloads': 2, 'midi_sysex': 2,
            'push_messaging': 2, 'ssl_cert_decisions': 2, 'metro_switch_to_desktop': 2,
            'protected_media_identifier': 2, 'app_banner': 2, 'site_engagement': 2,
            'durable_storage': 2
        }
    }
    
    chrome_options.add_experimental_option('prefs', prefs)
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument(f"--proxy-server={config.ROTATING_PROXY}")
    
    driver = webdriver.Chrome(options=chrome_options)
    
    try:
        driver.get(url)
        wait = WebDriverWait(driver, 5)
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".relative.main-list-results-item")))
        
        game_deals = []
        next_nr = 2
        
        while True:
            game_cards = driver.find_elements(By.CSS_SELECTOR, ".relative.main-list-results-item")
            
            for game_card in game_cards:
                try:
                    game_title = game_card.find_element(By.CSS_SELECTOR, "h3.bg-gradient-red").text
                    game_discount = game_card.find_element(By.CSS_SELECTOR, "div.main-list-results-item-discount").text.replace("%", "").replace("-", "")
                    game_price = game_card.find_element(By.CSS_SELECTOR, "div.main-list-results-item-price-new").text
                    game_link = game_card.find_element(By.CSS_SELECTOR, "figure.relative a").get_attribute("href")
                    game_affiliate_link = game_link + '?ref=mzvkywq'
                    game_image = game_card.find_element(By.CSS_SELECTOR, "figure.relative img.async-img-load.display-none").get_attribute("src")
                    
                    games = {
                        "source": "IndieGala",
                        "title": game_title,
                        "discount": int(game_discount),
                        "salePrice": game_price.replace(" ", ""),
                        "link": game_affiliate_link,
                        "image_link": game_image
                    }
                    game_deals.append(games)
                except Exception as e:
                    logger.debug(f"Error parsing game card: {e}")
            
            # Try to go to next page
            try:
                next_button = driver.find_element(By.CSS_SELECTOR, f"a[onclick*='/{next_nr}']")
                
                if next_button.is_displayed():
                    logger.info(f"Clicking page {next_nr}")
                    driver.execute_script("arguments[0].click();", next_button)
                    next_nr += 1
                    wait.until(EC.staleness_of(game_cards[0]))
                    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".relative.main-list-results-item")))
                else:
                    logger.info("No more pages")
                    break
            except Exception as e:
                logger.info(f"Finished scraping at page {next_nr-1}")
                break
                
    finally:
        driver.quit()
    
    logger.info(f"Scraped {len(game_deals)} deals from IndieGala")
    
    # Filter deals
    filtered_deals = _filter_indiegala_deals(game_deals)
    
    # Append to valid deals
    _append_to_valid_deals(filtered_deals)
    
    logger.info(f"Added {len(filtered_deals)} IndieGala deals to valid deals")
    return True

def _filter_indiegala_deals(game_deals):
    """
    Filter IndieGala deals by Steam top sellers and posted games
    
    Args:
        game_deals: List of scraped deals
    
    Returns:
        list: Filtered deals
    """
    # Filter by Steam top 500
    try:
        with open(config.STEAMDB_CSV, encoding="utf-8") as f:
            steamdb_games = list(csv.reader(f))
        steamdb_titles = [row[0] for row in steamdb_games]
        game_deals = [game for game in game_deals if game["title"] in steamdb_titles]
    except Exception as e:
        logger.error(f"Error filtering by Steam games: {e}")
    
    # Remove posted games
    posted_games_list = load_posted_games(config.POSTED_GAMES_FILE)
    game_deals = [game for game in game_deals if game["title"] not in posted_games_list]
    
    # Compare prices with Steam
    try:
        with open(config.STEAMDB_CSV, encoding="utf-8") as f:
            steamdb_reader = csv.reader(f)
            steamdb_games = list(steamdb_reader)
        
        filtered = []
        for game in game_deals:
            for steamdb_game in steamdb_games:
                if game["title"] == steamdb_game[0]:
                    try:
                        steam_price = float(steamdb_game[1].replace("$", "").replace(",", ""))
                        game_price = float(game["salePrice"].replace("$", "").replace(" ", ""))
                        
                        if game_price < steam_price:
                            filtered.append(game)
                    except ValueError:
                        pass
                    break
        
        return filtered
        
    except Exception as e:
        logger.error(f"Error comparing prices: {e}")
        return game_deals

def _append_to_valid_deals(deals):
    """
    Append deals to valid_deals.json
    
    Args:
        deals: List of deals to append
    """
    try:
        # Ensure JSON directory exists
        os.makedirs(config.JSON_DIR, exist_ok=True)
        
        # Load existing valid deals
        valid_deals_list = load_json_file(config.VALID_DEALS_JSON)
        
        # Append new deals
        valid_deals_list.append(deals)
        
        # Save back
        save_json_file(config.VALID_DEALS_JSON, valid_deals_list)
        
    except Exception as e:
        logger.error(f"Error appending to valid deals: {e}")

