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
from database.db_connect import get_connection, close_connection, execute_query
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
    logger.info("Starting affiliate product collection (CJ + IndieGala)")
    
    # Step 1: Fetch CJ Affiliate data files
    logger.info("Step 1/3: Fetching CJ Affiliate products...")
    cj_data_files = _fetch_cj_data_files()
    if cj_data_files is None:
        logger.error("Failed to fetch CJ products")
        return False
    
    # Step 2: Fetch IndieGala products
    logger.info("Step 2/3: Fetching IndieGala products...")
    indiegala_products = _fetch_indiegala_data()
    if indiegala_products is None:
        logger.warning("Failed to fetch IndieGala products, continuing with CJ products only")
        indiegala_products = []
    
    # Step 3: Process all data and write to CSV
    logger.info("Step 3/3: Processing and writing all products to CSV...")
    success = _process_csv_files(cj_data_files, indiegala_products)
    
    # Cleanup temporary files
    _cleanup_temp_files()
    
    if success:
        logger.info("Affiliate product collection completed successfully")
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


# ============================================================================
# CJ AFFILIATE FUNCTIONS
# ============================================================================

def _fetch_cj_data_files():
    """
    Fetch CJ Affiliate products ZIP, extract, and return list of CSV files
    
    Returns:
        list: List of extracted CSV/TXT file paths, or None if failed
    """
    logger.info("Starting CJ Affiliate product fetch")
    
    # CJ HTTP credentials
    url_base = "https://datatransfer.cj.com"
    username = config.CJ_HTTP_USERNAME
    password = config.CJ_HTTP_PASSWORD
    
    if not username or not password:
        logger.error("CJ credentials not found in environment variables")
        return None
    
    # File path (update date as needed)
    today_str = datetime.now().strftime("%Y%m%d")
    file_path = f"/datatransfer/files/7609708/outgoing/productcatalog/306393/product_feedex-shopping-{today_str}.zip"
    url = url_base + file_path
    
    # Create directories
    os.makedirs(config.TEMP_DIR, exist_ok=True)
    os.makedirs(config.CSV_DIR, exist_ok=True)
    
    logger.info(f"Fetching: {url}")
    
    try:
        # Download file
        response = requests.get(url, auth=(username, password))
        
        if response.status_code != 200:
            logger.error(f"Failed to download. Status: {response.status_code}")
            return None
        
        # Save ZIP file
        out_file = os.path.join(config.TEMP_DIR, os.path.basename(file_path))
        with open(out_file, "wb") as f:
            f.write(response.content)
        logger.info(f"Downloaded and saved as {out_file}")
        
        # Extract ZIP file
        data_files = _extract_zip_file(out_file)
        if not data_files:
            return None
        
        logger.info(f"CJ Affiliate: Found {len(data_files)} data files")
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
            logger.info(f"Extracted {len(zip_ref.namelist())} files")
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
    
    logger.info(f"Found {len(data_files)} data files to process")
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
    logger.info("Starting IndieGala product scraping")
    
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
                    logger.info(f"Scraping page {next_nr} (found {len(game_products)} products so far)")
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
                    logger.info(f"No more pages found (checked up to page {next_nr-1})")
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
    
    logger.info(f"IndieGala: Scraped {len(game_products)} unique products (skipped {duplicate_count} duplicates)")
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
                logger.info(f"Processing CJ file: {os.path.basename(data_file)}")
                file_rows = 0
                
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
                            file_rows += 1
                            total_rows += 1
                            
                            # Store row data for database insertion
                            all_rows_data.append(out_row)
                    
                    logger.info(f"Processed {file_rows} rows from {os.path.basename(data_file)}")
                    
                except Exception as e:
                    logger.error(f"Error processing {data_file}: {e}")
            
            # Process IndieGala products
            if indiegala_products:
                logger.info(f"Writing {len(indiegala_products)} IndieGala products...")
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
                
                logger.info(f"Written {len(indiegala_products)} IndieGala products")
        
        logger.info(f"Combined CSV written to: {config.PRODUCTS_CSV}")
        logger.info(f"Total rows: {total_rows} (CJ: {total_rows - len(indiegala_products)}, IndieGala: {len(indiegala_products)})")
        
        # Now process database inserts
        if not all_rows_data:
            logger.warning("No row data collected - skipping database inserts")
        else:
            _insert_offers_to_database(db_connection, all_rows_data)
        
        close_connection(db_connection)
        return True
        
    except Exception as e:
        logger.error(f"Error creating combined CSV: {e}")
        import traceback
        traceback.print_exc()
        if 'db_connection' in locals():
            close_connection(db_connection)
        return False

def _insert_offers_to_database(db_connection, all_rows_data):
    """
    Insert affiliate offers into database using batch processing
    
    Args:
        db_connection: Database connection object
        all_rows_data: List of row dictionaries from CSV
    """
    logger.info(f"Preparing database inserts for {len(all_rows_data)} products...")
    
    # Step 1: Collect unique titles and program names for batch lookup
    logger.info("Collecting unique titles and program names...")
    unique_titles = set()
    unique_program_names = set()
    
    for row_data in all_rows_data:
        title = row_data.get("TITLE", "").strip()
        program_name = row_data.get("PROGRAM_NAME", "").strip()
        if title:
            unique_titles.add(title)
        if program_name:
            unique_program_names.add(program_name)
    
    logger.info(f"Found {len(unique_titles)} unique titles and {len(unique_program_names)} unique program names")
    
    # Step 2: Batch lookup all item_ids at once (case-insensitive)
    item_id_map = _batch_lookup_item_ids(db_connection, unique_titles)
    
    # Step 3: Batch lookup all distributor_ids at once
    distributor_id_map = _batch_lookup_distributor_ids(db_connection, unique_program_names)
    
    # Step 4: Process rows and prepare query values
    query_values, stats = _prepare_offer_inserts(all_rows_data, item_id_map, distributor_id_map)
    
    # Step 5: Batch insert into database
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
    logger.info(f"Batch looking up {len(unique_titles)} unique items...")
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
        
        # Get all unique normalized titles
        unique_normalized_titles = list(normalized_title_map.keys())
        
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
        matches_found = 0
        for normalized_csv, csv_titles in normalized_title_map.items():
            if normalized_csv in db_normalized_map:
                item_id = db_normalized_map[normalized_csv]
                # Map all CSV titles with this normalized form to the item_id
                for csv_title in csv_titles:
                    if csv_title not in item_id_map:  # Use first match if duplicates
                        item_id_map[csv_title] = item_id
                        matches_found += 1
        
        logger.info(f"Found {len(item_id_map)} matching items in database (normalized/fuzzy match)")
    except Exception as e:
        logger.error(f"Error batch looking up items: {e}")
        import traceback
        traceback.print_exc()
    
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
    logger.info(f"Batch looking up {len(unique_program_names)} unique distributors...")
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
        
        logger.info(f"Found {len(distributor_id_map)} matching distributors in database")
    except Exception as e:
        logger.error(f"Error batch looking up distributors: {e}")
        import traceback
        traceback.print_exc()
    
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
    logger.info("Processing rows with batch lookup results...")
    query_values = []
    skipped_count = 0
    missing_item_count = 0
    missing_distributor_count = 0
    missing_sale_price_count = 0
    price_conversion_failed_count = 0
    missing_essential_data_count = 0
    
    # Diagnostic: Track offers with sale_price that are being skipped
    offers_with_sale_price_but_skipped = {
        'missing_item': [],
        'missing_distributor': [],
        'price_conversion_failed': [],
        'missing_essential': []
    }
    
    for row_data in all_rows_data:
        title = row_data.get("TITLE", "").strip()
        program_name = row_data.get("PROGRAM_NAME", "").strip()
        affiliate_url = row_data.get("LINK", "").strip()
        image_url = row_data.get("IMAGE_LINK", "").strip()
        list_price = row_data.get("PRICE", "").strip()
        sale_price = row_data.get("SALE_PRICE", "").strip()
        
        has_sale_price = sale_price and sale_price.strip()
        
        # Skip if essential data is missing
        if not title or not program_name or not affiliate_url:
            skipped_count += 1
            missing_essential_data_count += 1
            if has_sale_price:
                offers_with_sale_price_but_skipped['missing_essential'].append({
                    'title': title[:50], 'program': program_name, 'reason': 'missing essential data'
                })
            continue
        
        # Fast dictionary lookup (no query!)
        item_id = item_id_map.get(title)
        if not item_id:
            missing_item_count += 1
            if has_sale_price:
                offers_with_sale_price_but_skipped['missing_item'].append({
                    'title': title[:50], 'program': program_name
                })
            continue
        
        distributor_id = distributor_id_map.get(program_name)
        if not distributor_id:
            missing_distributor_count += 1
            if has_sale_price:
                offers_with_sale_price_but_skipped['missing_distributor'].append({
                    'title': title[:50], 'program': program_name
                })
            continue
        
        # Skip if no sale_price (required)
        if not sale_price or not sale_price.strip():
            missing_sale_price_count += 1
            skipped_count += 1
            continue
        
        # Convert prices to float for database
        try:
            # Clean price strings: remove $, commas, and USD text (case-insensitive)
            list_price_clean = str(list_price).replace("$", "").replace(",", "").upper().replace("USD", "").strip() if list_price else ""
            sale_price_clean = str(sale_price).replace("$", "").replace(",", "").upper().replace("USD", "").strip() if sale_price else ""
            
            list_price_float = float(list_price_clean) if list_price_clean else None
            sale_price_float = float(sale_price_clean) if sale_price_clean else None
        except Exception as e:
            list_price_float = None
            sale_price_float = None
            price_conversion_failed_count += 1
            skipped_count += 1
            if has_sale_price:
                offers_with_sale_price_but_skipped['price_conversion_failed'].append({
                    'title': title[:50], 'program': program_name, 'sale_price': sale_price[:20], 'error': str(e)[:50]
                })
            continue
        
        # Skip if sale_price conversion failed or is None
        if sale_price_float is None:
            skipped_count += 1
            continue
        
        # Calculate discount (round to whole number)
        discount = _calculate_discount(list_price, sale_price)
        
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
    
    stats = {
        'total': len(all_rows_data),
        'valid': len(query_values),
        'skipped': skipped_count,
        'missing_item': missing_item_count,
        'missing_distributor': missing_distributor_count,
        'missing_sale_price': missing_sale_price_count,
        'price_conversion_failed': price_conversion_failed_count,
        'missing_essential': missing_essential_data_count
    }
    
    logger.info(f"Processed {stats['total']} rows: {stats['valid']} valid, {stats['skipped']} skipped")
    logger.info(f"  Breakdown: {stats['missing_item']} missing items, {stats['missing_distributor']} missing distributors, {stats['missing_sale_price']} missing sale_price, {stats['price_conversion_failed']} price conversion failed, {stats['missing_essential']} missing essential data")
    
    # Diagnostic: Log offers with sale_price that were skipped
    total_with_sale_price_skipped = (
        len(offers_with_sale_price_but_skipped['missing_item']) +
        len(offers_with_sale_price_but_skipped['missing_distributor']) +
        len(offers_with_sale_price_but_skipped['price_conversion_failed']) +
        len(offers_with_sale_price_but_skipped['missing_essential'])
    )
    
    if total_with_sale_price_skipped > 0:
        logger.warning(f"DIAGNOSTIC: {total_with_sale_price_skipped} offers with sale_price were skipped:")
        logger.warning(f"  - {len(offers_with_sale_price_but_skipped['missing_item'])} missing items in database")
        logger.warning(f"  - {len(offers_with_sale_price_but_skipped['missing_distributor'])} missing distributors")
        logger.warning(f"  - {len(offers_with_sale_price_but_skipped['price_conversion_failed'])} price conversion failed")
        logger.warning(f"  - {len(offers_with_sale_price_but_skipped['missing_essential'])} missing essential data")
        
        # Show sample titles that are missing items
        if offers_with_sale_price_but_skipped['missing_item']:
            logger.warning(f"  Sample titles missing from database (first 10):")
            for item in offers_with_sale_price_but_skipped['missing_item'][:10]:
                logger.warning(f"    - '{item['title']}' from {item['program']}")
    
    # Write missing game titles to file
    if offers_with_sale_price_but_skipped['missing_item']:
        _write_missing_titles_to_file(offers_with_sale_price_but_skipped['missing_item'])
    
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
        logger.info(f"Successfully inserted {len(query_values)} affiliate products into database")
        
        total_skipped = stats['skipped'] + stats['missing_item'] + stats['missing_distributor']
        if total_skipped > 0:
            logger.info(f"Skipped {total_skipped} products: {stats['skipped']} missing data, {stats['missing_item']} missing items, {stats['missing_distributor']} missing distributors")
    except Exception as e:
        logger.error(f"Error inserting affiliate products into database: {e}")
        import traceback
        traceback.print_exc()
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
        "YUPLAY": "Yuplay",
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
        total_occurrences = sum(info['count'] for info in title_info.values())
        
        logger.info(f"Wrote {unique_titles_count} unique missing game titles to {config.MISSING_TITLES_CSV}")
        logger.info(f"  Total occurrences: {total_occurrences} (some titles appear multiple times)")
        logger.info(f"  File location: {os.path.abspath(config.MISSING_TITLES_CSV)}")
        
    except Exception as e:
        logger.error(f"Error writing missing titles to file: {e}")
        import traceback
        traceback.print_exc()

def _cleanup_temp_files():
    """Clean up temporary files in product_files directory"""
    logger.info("Cleaning up temporary files...")
    
    for fname in os.listdir(config.TEMP_DIR):
        fpath = os.path.join(config.TEMP_DIR, fname)
        if os.path.isfile(fpath) and not fname.startswith("."):
            try:
                os.remove(fpath)
                logger.debug(f"Deleted: {fname}")
            except Exception as e:
                logger.warning(f"Could not delete {fname}: {e}")
    
    logger.info("Cleanup complete")
