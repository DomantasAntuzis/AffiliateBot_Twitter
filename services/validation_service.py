"""
Deal validation service
Validates affiliate deals by checking actual prices and availability
"""
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import json
import csv
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import os
import sys
import re

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import config
from utils.logger import logger
from utils.helpers import load_posted_games, save_json_file

def validate_deals_batch(deals):
    """
    Validate a batch of deals using parallel processing
    
    Args:
        deals: List of deal dictionaries
    
    Returns:
        list: List of valid deals (sorted by source)
    """
    # Load Steam prices for comparison
    steam_games = _load_steam_prices()
    
    # Load posted games to skip
    posted_games_list = load_posted_games(config.POSTED_GAMES_FILE)
    
    valid_deals = []
    
    # Create browser pool
    browser_pool = BrowserPool(pool_size=config.BROWSER_POOL_SIZE)
    
    try:
        # Use ThreadPoolExecutor for parallel validation
        with ThreadPoolExecutor(max_workers=config.VALIDATION_WORKERS, thread_name_prefix="DealValidator") as executor:
            # Submit all deals for processing
            future_to_deal = {
                executor.submit(_validate_deal_worker, deal, posted_games_list, steam_games, browser_pool): deal 
                for deal in deals
            }
            
            # Collect results as they complete
            for future in as_completed(future_to_deal):
                try:
                    result = future.result()
                    if result:
                        valid_deals.append(result)
                except Exception:
                    pass  # Silently skip failed deals
        
    finally:
        # Close all browsers when done
        browser_pool.close_all()
    
    # Post-process: remove duplicates and sort by source
    processed_deals = _postprocess_deals(valid_deals)
    
    # Save to JSON file
    save_json_file(config.VALID_DEALS_JSON, processed_deals)
    
    # Count total deals (not just source groups)
    total_deals = sum(len(group) for group in processed_deals)
    logger.info(f"Validated {total_deals} deals from {len(processed_deals)} sources")
    
    return processed_deals

def _validate_deal_worker(deal, posted_games_list, steam_games, browser_pool):
    """
    Worker function to validate a single deal
    
    Args:
        deal: Deal dictionary
        posted_games_list: List of already posted game titles
        steam_games: List of Steam games with prices
        browser_pool: Browser pool instance
    
    Returns:
        dict: Valid deal or None if invalid
    """
    driver = None
    try:
        # Skip if already posted
        if deal["title"] in posted_games_list:
            return None
        
        # Get driver from pool (with retry)
        max_retries = 10
        for _ in range(max_retries):
            driver = browser_pool.get_driver()
            if driver:
                break
            time.sleep(0.1)
        
        if not driver:
            return None
        
        # Validate the deal
        result = _validate_deal(deal, driver, steam_games)
        return result
        
    except Exception:
        return None
    finally:
        if driver:
            browser_pool.return_driver(driver)

def _validate_deal(deal, driver, steam_games):
    """
    Validate a single deal by checking actual price and discount
    
    Args:
        deal: Deal dictionary
        driver: Selenium WebDriver instance
        steam_games: List of Steam games with prices
    
    Returns:
        dict: Modified deal if valid, None if invalid
    """
    deal_title = deal["title"]
    deal_link = deal["link"]
    deal_source = deal["source"]
    
    try:
        # IndieGala doesn't need web scraping - data already in CSV
        if deal_source == "IndieGala":
            return _validate_indiegala_deal(deal, steam_games)
        
        # Other sources need to visit the website
        driver.get(deal_link)
        
        # Validate based on source
        if deal_source == "GOG.COM INT":
            return _validate_gog_deal(deal, driver, steam_games)
        elif deal_source == "GamersGate.com":
            return _validate_gamersgate_deal(deal, driver, steam_games)
        elif deal_source == "YUPLAY":
            return _validate_yuplay_deal(deal, driver, steam_games)
        else:
            return None
            
    except Exception:
        return None

def _validate_gog_deal(deal, driver, steam_games):
    """Validate GOG deal"""
    try:
        wait = WebDriverWait(driver, 10)
        discount_span = wait.until(EC.presence_of_element_located((By.CLASS_NAME, "product-actions-price__discount")))
        discounted_price_span = wait.until(EC.presence_of_element_located((By.CLASS_NAME, "product-actions-price__final-amount")))
        
        discount_text = driver.execute_script("return arguments[0].textContent;", discount_span).strip().replace('%', '').replace('-', '')
        discount = int(discount_text)
        discounted_price_text = driver.execute_script("return arguments[0].textContent;", discounted_price_span).strip()
        discounted_price = float(discounted_price_text)
        
        if discount and discounted_price and _compare_prices(deal["title"], discounted_price, steam_games):
            deal["discount"] = discount
            deal["salePrice"] = f"${discounted_price:.2f}"
            return deal
        else:
            return None
            
    except Exception:
        return None

def _validate_gamersgate_deal(deal, driver, steam_games):
    """Validate GamersGate deal"""
    try:
        driver.implicitly_wait(0)
        WebDriverWait(driver, 6, poll_frequency=0.1).until(
            lambda driver: driver.execute_script("return document.readyState") == "complete"
        )
        
        # Handle age verification if present
        try:
            WebDriverWait(driver, 8, poll_frequency=0.1).until(
                EC.any_of(
                    EC.presence_of_element_located((By.CSS_SELECTOR, '.select[data-name="age_year"]')),
                    EC.presence_of_element_located((By.CSS_SELECTOR, '.catalog-item--price span'))
                )
            )
        except TimeoutException:
            pass
        
        if driver.find_elements(By.CSS_SELECTOR, '.select[data-name="age_year"]'):
            _handle_gamersgate_age_verification(driver)
        
        wait = WebDriverWait(driver, 10)
        discount_element = wait.until(EC.presence_of_element_located((By.CLASS_NAME, "catalog-item--discount-value")))
        discount = int(discount_element.text.strip().replace('%', '').replace('-', ''))
        
        discounted_price_element = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".catalog-item--price span")))
        discounted_price_text = driver.execute_script("return arguments[0].textContent;", discounted_price_element).strip()
        price_numeric_text = discounted_price_text.replace('$', '').replace(',', '').strip()
        discounted_price = float(price_numeric_text)
        
        image_src = driver.find_element(By.CSS_SELECTOR, "div.catalog-item--image img").get_attribute("src")
        
        if discount > 0 and discounted_price and _compare_prices(deal["title"], discounted_price, steam_games):
            deal["discount"] = discount
            deal["salePrice"] = f"${discounted_price:.2f}"
            deal["image_link"] = image_src
            return deal
        else:
            return None
            
    except Exception:
        return None

def _validate_yuplay_deal(deal, driver, steam_games):
    """Validate YUPLAY deal"""
    try:
        wait = WebDriverWait(driver, 10)
        
        product_container = wait.until(EC.presence_of_element_located((By.CLASS_NAME, "product-second-container")))
        discount_element = product_container.find_element(By.CLASS_NAME, "catalog-item-discount-label")
        discounted_price_element = product_container.find_element(By.CLASS_NAME, "catalog-item-sale-price")
        
        if discount_element and discounted_price_element:
            discount_span = discount_element.find_elements(By.TAG_NAME, "span")
            
            if discount_span:
                discount = driver.execute_script("return arguments[0].textContent;", discount_span[0]).strip()
                discount_int = int(discount)
                price = driver.execute_script("return arguments[0].textContent;", discounted_price_element).replace('$', '').replace(',', '').strip()
                price_float = float(price)
                
                if discount_int > 0 and price_float and _compare_prices(deal["title"], price_float, steam_games):
                    deal["discount"] = discount_int
                    deal["salePrice"] = f"${price_float:.2f}"
                    return deal
        
        return None
        
    except Exception:
        return None

def _validate_indiegala_deal(deal, steam_games):
    """Validate IndieGala deal - Only compare existing deal price with Steam price, no scraping."""
    try:
        # Extract price from deal
        discounted_price = 0
        discount = 0

        # Prefer "salePrice", fallback to "PRICE"
        if "SALE_PRICE" in deal:
            try:
                price_str = str(deal["SALE_PRICE"]).replace("$", "").replace(",", "").replace(" ", "").strip()
                discounted_price = float(price_str) if price_str else 0
            except Exception:
                discounted_price = 0

        if discounted_price == 0 and "PRICE" in deal:
            try:
                price_str = str(deal["PRICE"]).replace("$", "").replace(",", "").replace(" ", "").strip()
                discounted_price = float(price_str) if price_str else 0
            except Exception:
                discounted_price = 0

        # Extract discount if present (check both uppercase DISCOUNT from CSV and lowercase discount)
        if "DISCOUNT" in deal and deal["DISCOUNT"]:
            try:
                discount = int(str(deal["DISCOUNT"]).strip())
            except Exception:
                discount = 0
        elif "discount" in deal:
            try:
                discount = int(deal["discount"])
            except Exception:
                discount = 0

        # If not present, discount can be 0 or not used
        if discounted_price > 0:
            if _compare_prices(deal["title"], discounted_price, steam_games):
                if discount > 0:
                    deal["discount"] = discount
                deal["salePrice"] = f"${discounted_price:.2f}"
                return deal

        return None

    except Exception:
        return None

def _handle_gamersgate_age_verification(driver):
    """Handle GamersGate age verification popup"""
    try:
        gate_wait = WebDriverWait(driver, 4, poll_frequency=0.1)
        
        dropdown_year = gate_wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, '.select[data-name="age_year"]')))
        dropdown_year.click()
        year_option = gate_wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, '.select[data-name="age_year"] a[data-value="2003"]')))
        year_option.click()
        
        dropdown_month = gate_wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, '.select[data-name="age_month"]')))
        dropdown_month.click()
        month_option = gate_wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, '.select[data-name="age_month"] a[data-value="1"]')))
        month_option.click()
        
        dropdown_day = gate_wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, '.select[data-name="age_day"]')))
        dropdown_day.click()
        day_option = gate_wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, '.select[data-name="age_day"] a[data-value="1"]')))
        day_option.click()
        
        submit_button = gate_wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, 'button[type="submit"]')))
        submit_button.click()
        
        WebDriverWait(driver, 6, poll_frequency=0.1).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, '.catalog-item--price span'))
        )
    except TimeoutException:
        pass

def _normalize_title_for_matching(title):
    """
    Normalize title for fuzzy matching by removing punctuation and normalizing whitespace
    Same logic as affiliate_service.py for consistency
    
    Args:
        title: Title string to normalize
    
    Returns:
        str: Normalized title for matching
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

def _compare_prices(title, price, steam_games):
    """
    Compare deal price with Steam price
    Only validates deals that are cheaper than Steam price
    
    Args:
        title: Game title
        price: Deal price to compare
        steam_games: List of Steam games [[title, price], [title, price], ...]
    
    Returns:
        bool: True if deal is cheaper than Steam, False otherwise
    """
    # Normalize title for matching (same as affiliate_service)
    normalized_title = _normalize_title_for_matching(title)
    
    for s_row in steam_games:
        if len(s_row) >= 2:
            steam_title = s_row[0]
            steam_price = s_row[1]  # decimal(10,2) or None
            
            # Normalize Steam title for matching
            normalized_steam = _normalize_title_for_matching(steam_title)
            
            # If titles match, compare prices
            if normalized_title == normalized_steam:
                # Skip if Steam price is 0.00 (free game) or None
                if steam_price is None or float(steam_price) == 0.00:
                    return False
                
                try:
                    # Compare: deal must be cheaper than Steam
                    if price < float(steam_price):
                        return True
                except (ValueError, TypeError):
                    return False
    
    return False

def _load_steam_prices():
    """Load Steam prices from database"""
    try:
        from database.db_connect import get_connection
        
        connection = get_connection()
        if not connection:
            return []
        
        cursor = connection.cursor()
        cursor.execute("SELECT title, price FROM topsellers ORDER BY id ASC")
        rows = cursor.fetchall()
        cursor.close()
        connection.close()
        
        # Convert to list format: [[title, price], [title, price], ...]
        # price is decimal(10,2) or None for free games
        return [[row[0], row[1]] for row in rows]
    except Exception as e:
        logger.error(f"Error loading Steam prices from database: {e}")
        return []

def _postprocess_deals(valid_deals):
    """
    Remove duplicates and sort deals by source
    
    Args:
        valid_deals: List of valid deals
    
    Returns:
        list: List of deal lists sorted by source
    """
    # Remove duplicates
    deal_counts = {}
    for deal in valid_deals:
        deal_key = f"{deal['title']}_{deal['source']}"
        deal_counts[deal_key] = deal_counts.get(deal_key, 0) + 1
    
    # Keep only deals that appear exactly once
    non_duplicate_deals = []
    for deal in valid_deals:
        deal_key = f"{deal['title']}_{deal['source']}"
        if deal_counts[deal_key] == 1:
            non_duplicate_deals.append(deal)
    
    # Sort by source
    sorted_deals = []
    different_sources = []
    for deal in non_duplicate_deals:
        if deal["source"] not in different_sources:
            different_sources.append(deal["source"])
    
    for source in different_sources:
        source_deals = [deal for deal in non_duplicate_deals if deal["source"] == source]
        sorted_deals.append(source_deals)
    
    return sorted_deals

def create_chrome_driver():
    """Create a Chrome driver with optimized settings"""
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument(f"--proxy-server={config.ROTATING_PROXY}")
    
    # Block specific resource types
    chrome_options.add_argument("--disable-images")
    chrome_options.add_argument("--disable-plugins")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--disable-dev-shm-usage")
    
    # Hide Chrome error messages
    chrome_options.add_argument("--log-level=3")
    chrome_options.add_argument("--silent")
    chrome_options.add_argument("--disable-logging")
    chrome_options.add_argument("--disable-default-apps")
    chrome_options.add_argument("--disable-crash-reporter")
    chrome_options.add_argument("--disable-in-process-stack-traces")
    chrome_options.add_argument("--disable-background-timer-throttling")
    chrome_options.add_argument("--disable-renderer-backgrounding")
    chrome_options.add_argument("--disable-backgrounding-occluded-windows")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-logging"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    
    chrome_options.add_argument("--enable-features=NetworkService")
    chrome_options.add_argument("--disable-features=VizDisplayCompositor")
    chrome_options.page_load_strategy = 'eager'
    
    # Suppress Chrome error output
    with open(os.devnull, 'w') as devnull:
        old_stderr = sys.stderr
        sys.stderr = devnull
        try:
            driver = webdriver.Chrome(options=chrome_options)
        finally:
            sys.stderr = old_stderr
    
    # Block specific URL patterns
    try:
        driver.execute_cdp_cmd('Network.enable', {})
        driver.execute_cdp_cmd('Network.setBlockedURLs', {
            'urls': [
                '*.jpg', '*.jpeg', '*.png', '*.gif', '*.css', '*.svg',
                '*.woff', '*.woff2', '*.ttf', '*.eot',
                '*/fonts/*', '*font*', '*analytics*', '*tracking*', '*ads*'
            ]
        })
    except Exception:
        pass
    
    return driver

class BrowserPool:
    """Pool of persistent browsers for reuse"""
    def __init__(self, pool_size=3):
        self.drivers = []
        self.available_drivers = []
        self.lock = threading.Lock()
        
        # Create pool of browsers
        for i in range(pool_size):
            driver = create_chrome_driver()
            self.drivers.append(driver)
            self.available_drivers.append(driver)
    
    def get_driver(self):
        """Get an available driver from the pool"""
        with self.lock:
            if self.available_drivers:
                return self.available_drivers.pop()
            else:
                return None
    
    def return_driver(self, driver):
        """Return driver to the pool for reuse"""
        with self.lock:
            if driver in self.drivers:
                self.available_drivers.append(driver)
    
    def close_all(self):
        """Close all drivers in the pool"""
        with self.lock:
            for driver in self.drivers:
                try:
                    driver.quit()
                except Exception:
                    pass
            self.drivers.clear()
            self.available_drivers.clear()

