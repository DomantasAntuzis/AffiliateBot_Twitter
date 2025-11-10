"""
Affiliate service - handles fetching and processing affiliate product data
Supports CJ Affiliate API and IndieGala web scraping
"""
import requests
import os
import zipfile
import csv
import time
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
    # file_path = f"/datatransfer/files/7609708/outgoing/productcatalog/306393/product_feedex-shopping-{today_str}.zip"
    file_path = f"/datatransfer/files/7609708/outgoing/productcatalog/306393/product_feedex-shopping-20251109.zip"
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

def _process_csv_files(cj_data_files, indiegala_products=None):
    """
    Process CSV files and IndieGala products, combine into single organized CSV
    
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
                logger.info(f"Written {len(indiegala_products)} IndieGala products")
        
        logger.info(f"Combined CSV written to: {config.PRODUCTS_CSV}")
        logger.info(f"Total rows: {total_rows} (CJ: {total_rows - len(indiegala_products)}, IndieGala: {len(indiegala_products)})")
        return True
        
    except Exception as e:
        logger.error(f"Error creating combined CSV: {e}")
        return False

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

def _fetch_indiegala_data():
    """
    Scrape IndieGala products and return list of products
    
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
    
    logger.info(f"IndieGala: Scraped {len(game_products)} products")
    return game_products

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

