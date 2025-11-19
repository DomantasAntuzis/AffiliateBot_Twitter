"""
Affiliate service - handles fetching and processing affiliate product data
Supports CJ Affiliate API and IndieGala web scraping
"""
import requests
import os
import zipfile
import csv
import time
import re
from datetime import datetime
import sys
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import config
from database.db_connect import get_connection, execute_query
from utils.logger import logger


# ============================================================================
# PUBLIC API FUNCTIONS
# ============================================================================

def fetch_all_affiliate_products():
    """
    Fetch all affiliate products (CJ Affiliate + IndieGala) and write to CSV
    Collects all data first, then writes everything at once
    
    Returns:
        bool: True if successful, False otherwise
    """
    # Step 1: Fetch CJ Affiliate data files
    cj_data_files = _fetch_cj_data_files()
    if cj_data_files is None:
        logger.error("Failed to fetch CJ products")
        return False
    
    # Step 2: Fetch IndieGala products
    indiegala_products = _fetch_indiegala_data()
    if indiegala_products is None:
        indiegala_products = []
    
    # Step 3: Process all data and write to CSV
    success = _process_csv_files(cj_data_files, indiegala_products)
    
    # Cleanup temporary files
    _cleanup_temp_files()
    
    return success

def get_affiliate_products_from_csv():
    """
    Read and return affiliate products from CSV
    
    Returns:
        list: List of product dictionaries
    """
    products = []
    
    try:
        with open(config.PRODUCTS_CSV, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            products = list(reader)
        
        logger.info(f"Loaded {len(products)} products from CSV")
        return products
        
    except FileNotFoundError:
        logger.error(f"Products CSV not found: {config.PRODUCTS_CSV}")
        return []
    except Exception as e:
        logger.error(f"Error reading products CSV: {e}")
        return []

def _fetch_gamersgate_page(session, page=1, platform="pc", timestamp=None):
    """
    Fetch a single page of GamersGate offers from their API using session
    
    Args:
        session: requests.Session object (maintains same proxy IP)
        page: Page number (default: 1)
        platform: Platform filter (default: "pc")
        timestamp: Session timestamp for consistency
    
    Returns:
        dict: JSON response data or None on failure
    """
    if timestamp is None:
        timestamp = int(time.time() * 1000)
    
    url = "https://www.gamersgate.com/api/offers/"
    params = {
        "platform": platform,
        "timestamp": timestamp,
        "need_change_browser_url": "true",
        "activations": 1
    }
    
    # Only add page param if page > 1 (matching browser behavior)
    if page > 1:
        params["page"] = page
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json",
        "Referer": "https://www.gamersgate.com/offers/",
    }
    
    try:
        response = session.get(url, params=params, headers=headers, timeout=30)
        response.raise_for_status()
        data = response.json()
        return data
    except Exception as e:
        logger.warning(f"Error fetching GamersGate page {page}: {str(e)[:100]}")
        return None

def _parse_gamersgate_item(item):
    """
    Parse a GamersGate catalog item and extract relevant fields
    
    Args:
        item: Raw item dictionary from API
    
    Returns:
        dict: Parsed item with name, discount, prices, availability
    """
    import html
    
    # Clean prices (remove HTML entities and currency symbols)
    baseprice = item.get("baseprice", "")
    if baseprice:
        baseprice = html.unescape(baseprice)
        # Remove common currency symbols (€, $, £, ¥, etc.) and HTML entities
        baseprice = baseprice.replace("&nbsp;", " ").replace("€", "").replace("$", "").replace("£", "").replace("¥", "").strip()
    
    raw_price = item.get("raw_price", "").strip()
    if raw_price:
        # Remove currency symbols from raw_price too
        raw_price = raw_price.replace("€", "").replace("$", "").replace("£", "").replace("¥", "").strip()
    
    parsed = {
        "name": item.get("name", "").strip(),
        "discount_percent": item.get("discount_percent", 0),
        "raw_price": raw_price,
        "is_available": item.get("is_available", False),
        "baseprice": baseprice,
    }
    
    return parsed

def insert_gamersgate_offers():
    """
    Fetch GamersGate offers from API, match with affiliate products, and insert into database.
    Uses requests.Session() to maintain consistent proxy IP across all requests.
    Only inserts offers for games that exist in affiliate products CSV.
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        logger.info("Fetching GamersGate offers from API...")
        
        # Setup proxy
        proxies = {}
        if hasattr(config, 'ROTATING_PROXY') and config.ROTATING_PROXY:
            proxy_url = str(config.ROTATING_PROXY).split(',')[0].split(';')[0].strip()
            proxies = {
                'http': proxy_url,
                'https': proxy_url
            }
            logger.info(f"Using proxy for GamersGate API")
        
        # Create a Session object to maintain same connection (and same proxy IP)
        session = requests.Session()
        if proxies:
            session.proxies.update(proxies)
        
        # Verify proxy IP before starting
        if proxies:
            try:
                logger.info("Verifying proxy IP...")
                ip_check = session.get("https://api.ipify.org?format=json", timeout=10)
                actual_ip = ip_check.json().get('ip', 'Unknown')
                logger.info(f"GamersGate requests using proxy IP: {actual_ip}")
            except Exception as e:
                logger.warning(f"Could not verify proxy IP: {e}")
        
        # Fetch all GamersGate offers
        all_gamersgate_offers = []
        platform = "pc"
        page = 1
        previous_page_names = None
        # Use shared timestamp for consistent pagination (like browsers do)
        session_timestamp = int(time.time() * 1000)
        
        consecutive_failures = 0
        max_consecutive_failures = 3
        
        while True:
            # Use shared timestamp and session for all pages to ensure consistent catalog snapshot
            data = _fetch_gamersgate_page(
                session=session,
                page=page,
                platform=platform,
                timestamp=session_timestamp
            )
            if not data:
                consecutive_failures += 1
                if consecutive_failures >= max_consecutive_failures:
                    logger.warning(f"GamersGate: Stopped after {max_consecutive_failures} failed page requests")
                    break
                page += 1
                time.sleep(1)
                continue
            
            catalog = data.get("catalog", [])
            if not catalog:
                break
            
            parsed_items = [_parse_gamersgate_item(i) for i in catalog]
            current_names = [item.get("name") for item in parsed_items]
            
            # Check for duplicate (compare first 5 items to be less strict)
            if previous_page_names is not None:
                # Compare first 5 items instead of all items for more robust detection
                current_sample = current_names[:5] if len(current_names) >= 5 else current_names
                previous_sample = previous_page_names[:5] if len(previous_page_names) >= 5 else previous_page_names
                
                if current_sample == previous_sample:
                    break
            
            all_gamersgate_offers.extend(parsed_items)
            previous_page_names = current_names
            consecutive_failures = 0  # Reset on success
            page += 1
            time.sleep(1.5)
        
        # Close session
        session.close()
        
        logger.info(f"Collected {len(all_gamersgate_offers)} GamersGate offers from {page-1} pages")
        
        # Get affiliate products from CSV
        affiliate_products = get_affiliate_products_from_csv()
        if not affiliate_products:
            logger.error("No affiliate products found in CSV")
            return False
        
        # Create mapping: normalized affiliate title -> affiliate product data
        affiliate_map = {}
        for product in affiliate_products:
            title = product.get("TITLE", "").strip()
            program_name = product.get("PROGRAM_NAME", "").strip()
            if title and program_name == "GamersGate.com":
                normalized_title = _normalize_title_for_matching(title)
                # Store first match (or could store list if duplicates)
                if normalized_title not in affiliate_map:
                    affiliate_map[normalized_title] = product
        
        logger.info(f"Found {len(affiliate_map)} GamersGate affiliate products in CSV")
        
        # Match GamersGate offers with affiliate products
        matched_offers = []
        for gg_offer in all_gamersgate_offers:
            gg_title = gg_offer.get("name", "").strip()
            if not gg_title:
                continue
            
            # Skip if not available
            if not gg_offer.get("is_available", False):
                continue
            
            # Skip if no sale price
            if not gg_offer.get("raw_price", "").strip():
                continue
            
            normalized_gg_title = _normalize_title_for_matching(gg_title)
            affiliate_product = affiliate_map.get(normalized_gg_title)
            
            if affiliate_product:
                # Get prices
                sale_price = gg_offer.get("raw_price", "").strip()
                baseprice = gg_offer.get("baseprice", "").strip()
                discount_percent = gg_offer.get("discount_percent", 0)
                
                # If no baseprice but we have discount_percent, calculate it
                if not baseprice and discount_percent > 0 and sale_price:
                    try:
                        sale_float = float(sale_price.replace("$", "").replace(",", "").strip())
                        # discount_percent = (baseprice - sale_price) / baseprice * 100
                        # So: baseprice = sale_price / (1 - discount_percent/100)
                        baseprice = str(round(sale_float / (1 - discount_percent / 100), 2))
                    except:
                        pass
                
                # Only add if we have both prices
                if baseprice and sale_price:
                    matched_offers.append({
                        "TITLE": gg_title,  # Use GamersGate title
                        "PROGRAM_NAME": "GamersGate.com",
                        "LINK": affiliate_product.get("LINK", "").strip(),
                        "IMAGE_LINK": affiliate_product.get("IMAGE_LINK", "").strip(),
                        "PRICE": baseprice,  # List price (calculated if needed)
                        "SALE_PRICE": sale_price,  # Sale price
                        "DISCOUNT": str(discount_percent)
                    })
        
        logger.info(f"Matched {len(matched_offers)} GamersGate offers with affiliate products")
        
        if not matched_offers:
            logger.warning("No matched GamersGate offers to insert")
            return True
        
        # Insert matched offers into database
        db_connection = get_connection()
        if not db_connection:
            logger.error("Failed to connect to database")
            return False
        
        try:
            _insert_offers_to_database(db_connection, matched_offers)
            logger.info(f"Successfully processed GamersGate offers")
            return True
        except Exception as e:
            logger.error(f"Error inserting GamersGate offers: {e}")
            return False
        finally:
            if db_connection and db_connection.is_connected():
                db_connection.close()
        
    except Exception as e:
        logger.error(f"Error in insert_gamersgate_offers: {e}")
        return False


# ============================================================================
# CJ AFFILIATE FUNCTIONS
# ============================================================================

def _fetch_cj_data_files():
    """
    Fetch CJ Affiliate products ZIP, extract, and return list of CSV files
    
    Returns:
        list: List of extracted CSV/TXT file paths, or None if failed
    """
    # CJ HTTP credentials
    url_base = "https://datatransfer.cj.com"
    username = config.CJ_HTTP_USERNAME
    password = config.CJ_HTTP_PASSWORD
    
    if not username or not password:
        logger.error("CJ credentials not found in environment variables")
        return None
    
    today_str = datetime.now().strftime("%Y%m%d")
    file_path = f"/datatransfer/files/7609708/outgoing/productcatalog/306393/product_feedex-shopping-{today_str}.zip"
    # file_path = f"/datatransfer/files/7609708/outgoing/productcatalog/306393/product_feedex-shopping-20251116.zip"
    url = url_base + file_path
    
    # Create directories
    os.makedirs(config.TEMP_DIR, exist_ok=True)
    os.makedirs(config.CSV_DIR, exist_ok=True)
    
    try:
        # Download file
        response = requests.get(url, auth=(username, password))
        
        if response.status_code != 200:
            logger.error(f"Failed to download CJ products. Status: {response.status_code}")
            return None
        
        # Save ZIP file
        out_file = os.path.join(config.TEMP_DIR, os.path.basename(file_path))
        with open(out_file, "wb") as f:
            f.write(response.content)
        
        # Extract ZIP file
        data_files = _extract_zip_file(out_file)
        if not data_files:
            return None
        
        logger.info(f"CJ Affiliate: Fetched {len(data_files)} data files")
        return data_files
        
    except Exception as e:
        logger.error(f"Error fetching CJ products: {e}")
        return None

def _extract_zip_file(zip_path):
    """
    Extract ZIP file and return list of CSV/TXT files
    
    Args:
        zip_path: Path to ZIP file
    
    Returns:
        list: List of extracted CSV/TXT file paths
    """
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(config.TEMP_DIR)
    except zipfile.BadZipFile:
        logger.error("The downloaded file is not a valid zip archive")
        return []
    
    # Find all CSV/TXT files
    data_files = []
    for fname in os.listdir(config.TEMP_DIR):
        if fname.lower().endswith((".csv", ".txt")):
            fpath = os.path.join(config.TEMP_DIR, fname)
            if os.path.isfile(fpath):
                data_files.append(fpath)
    
    if not data_files:
        logger.error("No CSV or TXT files found in extracted archive")
        return []
    
    return data_files


# ============================================================================
# INDIEGALA SCRAPING FUNCTIONS
# ============================================================================

def _fetch_indiegala_data():
    """
    Scrape IndieGala products and return list of products
    Deduplicates products by title to prevent duplicates
    
    Returns:
        list: List of product dictionaries, or None if failed
    """
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
    
    # Set timeouts for faster page loads
    driver.set_page_load_timeout(20)  # Max 20 seconds per page
    driver.implicitly_wait(2)  # Reduced implicit wait
    
    try:
        driver.get(url)
        wait = WebDriverWait(driver, 5)  # Increased timeout for initial load
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".relative.main-list-results-item")))
        
        game_products = []
        seen_titles = set()  # Track seen game titles to prevent duplicates
        duplicate_count = 0
        next_nr = 2
        consecutive_failures = 0
        max_failures = 3
        
        while True:
            game_cards = driver.find_elements(By.CSS_SELECTOR, ".relative.main-list-results-item")
            
            if not game_cards:
                logger.warning(f"No game cards found on page, stopping")
                break
                        
            for game_card in game_cards:
                try:
                    game_title = game_card.find_element(By.CSS_SELECTOR, "h3.bg-gradient-red").text
                    
                    # Skip if we've already seen this title (deduplication)
                    if game_title in seen_titles:
                        duplicate_count += 1
                        continue
                    seen_titles.add(game_title)
                    
                    game_discount = game_card.find_element(By.CSS_SELECTOR, "div.main-list-results-item-discount").text.replace("%", "").replace("-", "")
                    game_price = game_card.find_element(By.CSS_SELECTOR, "div.main-list-results-item-price-new").text
                    game_link = game_card.find_element(By.CSS_SELECTOR, "figure.relative a").get_attribute("href")
                    game_affiliate_link = game_link + '?ref=mzvkywq'
                    
                    # Try to get image, but don't fail if not available
                    try:
                        game_image = game_card.find_element(By.CSS_SELECTOR, "figure.relative img.async-img-load.display-none").get_attribute("src")
                    except:
                        game_image = ""
                    
                    # Extract original price if available (for SALE_PRICE vs PRICE)
                    original_price = ""
                    try:
                        original_price_elem = game_card.find_element(By.CSS_SELECTOR, "div.main-list-results-item-price-old")
                        original_price = original_price_elem.text.replace(" ", "")
                    except:
                        pass
                    
                    # Parse discount percentage
                    try:
                        discount_percent = int(game_discount) if game_discount else 0
                    except:
                        discount_percent = 0
                    
                    product = {
                        "PROGRAM_NAME": "IndieGala",
                        "ID": f"IG-{game_title.replace(' ', '-').replace(':', '')[:50]}",  # Generate simple ID
                        "TITLE": game_title,
                        "LINK": game_affiliate_link,
                        "IMAGE_LINK": game_image,
                        "AVAILABILITY": "in stock",
                        "PRICE": original_price if original_price else game_price.replace(" ", ""),
                        "SALE_PRICE": game_price.replace(" ", ""),
                        "DISCOUNT": str(discount_percent) if discount_percent > 0 else ""
                    }
                    game_products.append(product)
                except Exception as e:
                    logger.debug(f"Error parsing game card: {e}")
            
            # Try to go to next page - IMPROVED PAGINATION LOGIC
            try:
                # Try multiple selector patterns for next button
                next_button = None
                selectors = [
                    f"a[onclick*='/{next_nr}']",
                    f"a[href*='/{next_nr}']",
                    f"a:contains('{next_nr}')"
                ]
                
                for selector in selectors:
                    try:
                        next_button = driver.find_element(By.CSS_SELECTOR, selector)
                        if next_button and next_button.is_displayed():
                            break
                    except:
                        continue
                
                if next_button and next_button.is_displayed():
                    driver.execute_script("arguments[0].click();", next_button)
                    next_nr += 1
                    consecutive_failures = 0
                    
                    # Wait for new content with longer timeout
                    try:
                        WebDriverWait(driver, 5).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, ".relative.main-list-results-item"))
                        )
                        # Additional small wait to ensure page is fully loaded
                        time.sleep(0.5)
                    except Exception as wait_error:
                        logger.warning(f"Wait timeout on page {next_nr-1}, but continuing...")
                        consecutive_failures += 1
                        if consecutive_failures >= max_failures:
                            logger.warning(f"Too many consecutive failures ({consecutive_failures}), stopping pagination")
                            break
                else:
                    break
            except Exception as e:
                consecutive_failures += 1
                logger.warning(f"Error navigating to page {next_nr}: {e}")
                if consecutive_failures >= max_failures:
                    logger.warning(f"Too many consecutive failures ({consecutive_failures}), stopping pagination")
                    break
                # Try to continue with current page
                continue
                
    except Exception as e:
        logger.error(f"Error scraping IndieGala: {e}")
        return None
    finally:
        driver.quit()
    
    logger.info(f"IndieGala: Fetched {len(game_products)} products")
    return game_products


# ============================================================================
# CSV PROCESSING & DATABASE INSERTION FUNCTIONS
# ============================================================================

def _process_csv_files(cj_data_files, indiegala_products=None):
    """
    Process CSV files and IndieGala products, combine into single organized CSV
    Also inserts affiliate product data into database
    
    Args:
        cj_data_files: List of CSV file paths from CJ Affiliate
        indiegala_products: List of IndieGala product dictionaries (optional)
    
    Returns:
        bool: True if successful, False otherwise
    """
    fields = ["PROGRAM_NAME", "ID", "TITLE", "LINK", "IMAGE_LINK", "AVAILABILITY", "PRICE", "SALE_PRICE", "DISCOUNT"]
    
    if indiegala_products is None:
        indiegala_products = []
    
    try:
        db_connection = get_connection()
        all_rows_data = []  # Store all row data for batch processing
        
        with open(config.PRODUCTS_CSV, "w", newline='', encoding="utf-8") as outfile:
            writer = csv.DictWriter(outfile, fieldnames=fields, quoting=csv.QUOTE_MINIMAL)
            writer.writeheader()
            
            total_rows = 0
            
            # Process CJ Affiliate CSV files
            for data_file in cj_data_files:
                try:
                    with open(data_file, newline='', encoding="utf-8") as infile:
                        reader = csv.DictReader(infile)
                        for row in reader:
                            # Clean row data
                            out_row = {
                                field: row.get(field, "")
                                    .replace("\n", " ")
                                    .replace("\r", " ")
                                    .replace("\t", " ")
                                    .strip()
                                for field in fields
                            }

                            # CJ Affiliate products don't have discount, set to empty
                            if "DISCOUNT" not in out_row or not out_row["DISCOUNT"]:
                                out_row["DISCOUNT"] = ""
                            
                            writer.writerow(out_row)
                            total_rows += 1
                            
                            # Store row data for database insertion
                            all_rows_data.append(out_row)
                    
                except Exception as e:
                    logger.error(f"Error processing {data_file}: {e}")
            
            # Process IndieGala products
            if indiegala_products:
                for product in indiegala_products:
                    # Clean product data
                    out_row = {
                        field: str(product.get(field, ""))
                            .replace("\n", " ")
                            .replace("\r", " ")
                            .replace("\t", " ")
                            .strip()
                        for field in fields
                    }
                    writer.writerow(out_row)
                    total_rows += 1
                    
                    # Store row data for database insertion
                    all_rows_data.append(out_row)
        
        logger.info(f"Processed {total_rows} products ({total_rows - len(indiegala_products)} CJ, {len(indiegala_products)} IndieGala)")
        
        # Now process database inserts
        if not all_rows_data:
            logger.warning("No row data collected - skipping database inserts")
        else:
            _insert_offers_to_database(db_connection, all_rows_data)
        
        db_connection.close()
        return True
        
    except Exception as e:
        logger.error(f"Error creating combined CSV: {e}")
        if 'db_connection' in locals():
            db_connection.close()
        return False

def _insert_offers_to_database(db_connection, all_rows_data):
    """
    Insert affiliate offers into database using batch processing
    
    Args:
        db_connection: Database connection object
        all_rows_data: List of row dictionaries from CSV
    """
    # Collect unique titles and program names for batch lookup
    unique_titles = set()
    unique_program_names = set()
    
    for row_data in all_rows_data:
        title = row_data.get("TITLE", "").strip()
        program_name = row_data.get("PROGRAM_NAME", "").strip()
        if title:
            unique_titles.add(title)
        if program_name:
            unique_program_names.add(program_name)
    
    # Batch lookup all item_ids and distributor_ids
    item_id_map = _batch_lookup_item_ids(db_connection, unique_titles)
    distributor_id_map = _batch_lookup_distributor_ids(db_connection, unique_program_names)
    
    # Process rows and prepare query values
    query_values, stats = _prepare_offer_inserts(all_rows_data, item_id_map, distributor_id_map)
    
    # Batch insert into database
    if query_values:
        _execute_batch_insert(db_connection, query_values, stats)
    else:
        logger.warning("No valid products to insert into database")

def _batch_lookup_item_ids(db_connection, unique_titles):
    """
    Batch lookup item_ids for all unique titles (fuzzy matching with normalization)
    Handles punctuation differences like hyphens, colons, etc.
    
    Args:
        db_connection: Database connection object
        unique_titles: Set of unique titles
    
    Returns:
        dict: Mapping of title -> item_id
    """
    item_id_map = {}
    normalized_title_map = {}  # Map normalized title -> list of original CSV titles
    
    if not unique_titles:
        return item_id_map
    
    try:
        # Create normalized mapping: normalized CSV title -> original CSV titles
        for csv_title in unique_titles:
            normalized = _normalize_title_for_matching(csv_title)
            if normalized not in normalized_title_map:
                normalized_title_map[normalized] = []
            normalized_title_map[normalized].append(csv_title)
        
        # Fetch all items from database and normalize them for matching
        cursor = db_connection.cursor(buffered=True)
        cursor.execute("SELECT id, title FROM items")
        db_items = cursor.fetchall()
        cursor.close()
        
        # Create normalized database title -> item_id mapping
        db_normalized_map = {}
        for item_id, db_title in db_items:
            normalized_db_title = _normalize_title_for_matching(db_title)
            # If multiple DB items have same normalized title, use first one
            if normalized_db_title not in db_normalized_map:
                db_normalized_map[normalized_db_title] = item_id
        
        # Match normalized CSV titles with normalized DB titles
        for normalized_csv, csv_titles in normalized_title_map.items():
            if normalized_csv in db_normalized_map:
                item_id = db_normalized_map[normalized_csv]
                # Map all CSV titles with this normalized form to the item_id
                for csv_title in csv_titles:
                    if csv_title not in item_id_map:  # Use first match if duplicates
                        item_id_map[csv_title] = item_id
        
    except Exception as e:
        logger.error(f"Error batch looking up items: {e}")
    
    return item_id_map

def _batch_lookup_distributor_ids(db_connection, unique_program_names):
    """
    Batch lookup distributor_ids for all unique program names
    
    Args:
        db_connection: Database connection object
        unique_program_names: Set of unique program names
    
    Returns:
        dict: Mapping of program_name -> distributor_id
    """
    distributor_id_map = {}
    
    if not unique_program_names:
        return distributor_id_map
    
    try:
        # Normalize all program names and create mapping
        normalized_to_original = {}
        for prog_name in unique_program_names:
            normalized = _normalize_distributor_name(prog_name)
            if normalized not in normalized_to_original:
                normalized_to_original[normalized] = []
            normalized_to_original[normalized].append(prog_name)
        
        # Query with normalized names
        cursor = db_connection.cursor(buffered=True)
        format_strings = ','.join(['%s'] * len(normalized_to_original))
        cursor.execute(
            f"SELECT id, name FROM distributors WHERE name IN ({format_strings})",
            tuple(normalized_to_original.keys())
        )
        results = cursor.fetchall()
        cursor.close()
        
        # Create mapping: original program_name -> distributor_id
        for row in results:
            dist_id, normalized_name = row[0], row[1]
            original_names = normalized_to_original.get(normalized_name, [])
            for original_name in original_names:
                distributor_id_map[original_name] = dist_id
        
    except Exception as e:
        logger.error(f"Error batch looking up distributors: {e}")
    
    return distributor_id_map

def _prepare_offer_inserts(all_rows_data, item_id_map, distributor_id_map):
    """
    Prepare offer insert values from row data
    
    Args:
        all_rows_data: List of row dictionaries
        item_id_map: Mapping of title -> item_id
        distributor_id_map: Mapping of program_name -> distributor_id
    
    Returns:
        tuple: (query_values list, stats dict)
    """
    query_values = []
    skipped_count = 0
    missing_item_count = 0
    missing_distributor_count = 0
    missing_sale_price_count = 0
    price_conversion_failed_count = 0
    missing_essential_data_count = 0
    
    # Track stats per distributor for debugging
    distributor_stats = {}
    
    for row_data in all_rows_data:
        title = row_data.get("TITLE", "").strip()
        program_name = row_data.get("PROGRAM_NAME", "").strip()
        affiliate_url = row_data.get("LINK", "").strip()
        image_url = row_data.get("IMAGE_LINK", "").strip()
        list_price = row_data.get("PRICE", "").strip()
        sale_price = row_data.get("SALE_PRICE", "").strip()

        # Initialize distributor stats if not exists
        if program_name and program_name not in distributor_stats:
            distributor_stats[program_name] = {
                'total': 0, 'valid': 0, 'missing_essential': 0, 'missing_item': 0,
                'missing_distributor': 0, 'missing_sale_price': 0,
                'price_conversion_failed': 0, 'discount_too_low': 0
            }
        
        if program_name:
            distributor_stats[program_name]['total'] += 1

        # Skip if essential data is missing
        if not title or not program_name or not affiliate_url:
            skipped_count += 1
            missing_essential_data_count += 1
            if program_name:
                distributor_stats[program_name]['missing_essential'] += 1
            continue
        
        # Fast dictionary lookup (no query!)
        item_id = item_id_map.get(title)
        if not item_id:
            missing_item_count += 1
            distributor_stats[program_name]['missing_item'] += 1
            continue
        
        distributor_id = distributor_id_map.get(program_name)
        if not distributor_id:
            missing_distributor_count += 1
            distributor_stats[program_name]['missing_distributor'] += 1
            continue
        
        # Skip if no sale_price (required)
        if not sale_price or not sale_price.strip():
            missing_sale_price_count += 1
            skipped_count += 1
            distributor_stats[program_name]['missing_sale_price'] += 1
            continue
        
        # Convert prices to float for database
        try:
            # Clean price strings: remove currency symbols ($, €, £, ¥), commas, and currency codes (USD, EUR, GBP, etc.)
            list_price_clean = str(list_price).replace("$", "").replace("€", "").replace("£", "").replace("¥", "").replace(",", "").upper().replace("USD", "").replace("EUR", "").replace("GBP", "").strip() if list_price else ""
            sale_price_clean = str(sale_price).replace("$", "").replace("€", "").replace("£", "").replace("¥", "").replace(",", "").upper().replace("USD", "").replace("EUR", "").replace("GBP", "").strip() if sale_price else ""
            
            list_price_float = float(list_price_clean) if list_price_clean else None
            sale_price_float = float(sale_price_clean) if sale_price_clean else None
        except Exception:
            list_price_float = None
            sale_price_float = None
            price_conversion_failed_count += 1
            skipped_count += 1
            distributor_stats[program_name]['price_conversion_failed'] += 1
            continue
        
        # Skip if sale_price conversion failed or is None
        if sale_price_float is None:
            skipped_count += 1
            distributor_stats[program_name]['price_conversion_failed'] += 1
            continue
        
        # Calculate discount (round to whole number) - use cleaned prices!
        discount = _calculate_discount(list_price_clean, sale_price_clean)
        
        # Skip if discount is less than 20% (minimum requirement for all distributors)
        if discount < 20:
            skipped_count += 1
            distributor_stats[program_name]['discount_too_low'] += 1
            continue
        
        # Default is_valid to True (1)
        is_valid = 1
        
        # Build query values tuple: (item_id, distributor_id, affiliate_url, image_url, list_price, sale_price, discount, is_valid)
        query_values.append((
            item_id,
            distributor_id,
            affiliate_url,
            image_url,
            list_price_float,
            sale_price_float,
            discount,
            is_valid
        ))
        distributor_stats[program_name]['valid'] += 1
    
    stats = {
        'total': len(all_rows_data),
        'valid': len(query_values),
        'skipped': skipped_count,
        'missing_item': missing_item_count,
        'missing_distributor': missing_distributor_count,
        'missing_sale_price': missing_sale_price_count,
        'price_conversion_failed': price_conversion_failed_count,
        'missing_essential': missing_essential_data_count,
        'distributor_stats': distributor_stats
    }
    
    # Log detailed stats for GamersGate, GOG, and YUPLAY to debug insertion issues
    for dist_name in ['GamersGate.com', 'GOG.COM INT', 'YUPLAY']:
        if dist_name in distributor_stats:
            stats_data = distributor_stats[dist_name]
            logger.info(f"{dist_name} stats: total={stats_data['total']}, valid={stats_data['valid']}, "
                      f"missing_item={stats_data['missing_item']}, missing_distributor={stats_data['missing_distributor']}, "
                      f"missing_sale_price={stats_data['missing_sale_price']}, "
                      f"price_conversion_failed={stats_data['price_conversion_failed']}, "
                      f"discount_too_low={stats_data['discount_too_low']}, "
                      f"missing_essential={stats_data['missing_essential']}")
    
    # Write missing game titles to file (only if there are missing items)
    if missing_item_count > 0:
        missing_items = []
        for row_data in all_rows_data:
            title = row_data.get("TITLE", "").strip()
            program_name = row_data.get("PROGRAM_NAME", "").strip()
            sale_price = row_data.get("SALE_PRICE", "").strip()
            
            if title and program_name and sale_price:
                item_id = item_id_map.get(title)
                if not item_id:
                    missing_items.append({'title': title, 'program': program_name})
        
        if missing_items:
            _write_missing_titles_to_file(missing_items)
    
    return query_values, stats

def _execute_batch_insert(db_connection, query_values, stats):
    """
    Execute batch insert of offers into database
    
    Args:
        db_connection: Database connection object
        query_values: List of tuples for executemany
        stats: Dictionary with statistics
    """
    try:
        cursor = db_connection.cursor(buffered=True)
        cursor.executemany(
            """INSERT INTO offers 
               (item_id, distributor_id, affiliate_url, image_url, list_price, sale_price, discount, is_valid) 
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
               ON DUPLICATE KEY UPDATE 
               affiliate_url = VALUES(affiliate_url),
               image_url = VALUES(image_url),
               list_price = VALUES(list_price),
               sale_price = VALUES(sale_price),
               discount = VALUES(discount),
               is_valid = VALUES(is_valid)""",
            query_values
        )
        db_connection.commit()
        cursor.close()
        logger.info(f"Inserted {len(query_values)} offers into database")
    except Exception as e:
        logger.error(f"Error inserting affiliate products into database: {e}")
        db_connection.rollback()


# ============================================================================
# HELPER/UTILITY FUNCTIONS
# ============================================================================

def _normalize_title_for_matching(title):
    """
    Normalize title for fuzzy matching by removing punctuation and normalizing whitespace
    Handles variations like:
    - "Cronos: The New Dawn - Deluxe Edition" vs "Cronos: The New Dawn Deluxe Edition"
    - Different hyphen styles, colons, etc.
    
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

def _normalize_distributor_name(program_name):
    """
    Normalize PROGRAM_NAME from CSV to match database distributor names
    Examples:
    - "GamersGate.com" → "GamersGate"
    - "YUPLAY" → "Yuplay" (or "YUPLAY" depending on DB)
    - "GOG.COM" → "GOG"
    - "IndieGala" → "IndieGala" (same)
    """
    program_name = program_name.strip()
    
    # Mapping from CSV PROGRAM_NAME to database name
    name_mapping = {
        "GamersGate.com": "GamersGate",
        "GOG.COM INT": "GOG",
        "YUPLAY": "YUPLAY",  # Database has YUPLAY in all caps
        "IndieGala": "IndieGala",
    }
    
    # Return mapped name if exists, otherwise return original
    return name_mapping.get(program_name, program_name)

def _calculate_discount(list_price_str, sale_price_str):
    """
    Calculate discount percentage from list_price and sale_price
    Returns rounded whole number percentage, or 0 if calculation fails
    """
    try:
        # Clean price strings
        list_price_str = str(list_price_str).replace("$", "").replace(",", "").replace(" ", "").strip()
        sale_price_str = str(sale_price_str).replace("$", "").replace(",", "").replace(" ", "").strip()
        
        if not list_price_str or not sale_price_str:
            return 0
        
        list_price = float(list_price_str)
        sale_price = float(sale_price_str)
        
        if list_price <= 0:
            return 0
        
        discount = ((list_price - sale_price) / list_price) * 100
        return round(discount)
    except Exception as e:
        logger.debug(f"Error calculating discount: {e}")
        return 0

def _write_missing_titles_to_file(missing_items):
    """
    Write missing game titles to CSV file for easy import
    
    Args:
        missing_items: List of dictionaries with 'title' and 'program' keys
    """
    try:
        # Deduplicate titles and collect distributors
        title_info = {}
        for item in missing_items:
            title = item['title']
            program = item['program']
            
            if title not in title_info:
                title_info[title] = {
                    'distributors': set(),
                    'count': 0
                }
            
            title_info[title]['distributors'].add(program)
            title_info[title]['count'] += 1
        
        # Sort by count (descending) to prioritize titles that appear more often
        sorted_titles = sorted(title_info.items(), key=lambda x: x[1]['count'], reverse=True)
        
        # Write to CSV
        os.makedirs(config.CSV_DIR, exist_ok=True)
        with open(config.MISSING_TITLES_CSV, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['TITLE', 'DISTRIBUTORS', 'OCCURRENCES'])
            
            for title, info in sorted_titles:
                distributors_str = ', '.join(sorted(info['distributors']))
                writer.writerow([title, distributors_str, info['count']])
        
        unique_titles_count = len(title_info)
        
        logger.info(f"Missing titles file created: {unique_titles_count} unique titles")
        
    except Exception as e:
        logger.error(f"Error writing missing titles to file: {e}")

def _cleanup_temp_files():
    """Clean up temporary files in product_files directory"""
    for fname in os.listdir(config.TEMP_DIR):
        fpath = os.path.join(config.TEMP_DIR, fname)
        if os.path.isfile(fpath) and not fname.startswith("."):
            try:
                os.remove(fpath)
            except Exception:
                pass
