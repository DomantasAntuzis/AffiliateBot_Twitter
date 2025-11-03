"""
CJ Affiliate API integration service
Handles fetching and processing affiliate product data
"""
import requests
import os
import zipfile
import csv
from datetime import datetime
import sys

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import config
from utils.logger import logger

def fetch_cj_products():
    """
    Fetch CJ Affiliate products, extract ZIP, and process CSV files
    
    Returns:
        bool: True if successful, False otherwise
    """
    logger.info("Starting CJ Affiliate product fetch")
    
    # CJ HTTP credentials
    url_base = "https://datatransfer.cj.com"
    username = config.CJ_HTTP_USERNAME
    password = config.CJ_HTTP_PASSWORD
    
    if not username or not password:
        logger.error("CJ credentials not found in environment variables")
        return False
    
    # File path (update date as needed)
    today_str = datetime.now().strftime("%Y%m%d")
    file_path = f"/datatransfer/files/7609708/outgoing/productcatalog/306393/product_feedex-shopping-20251102.zip"
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
            return False
        
        # Save ZIP file
        out_file = os.path.join(config.TEMP_DIR, os.path.basename(file_path))
        with open(out_file, "wb") as f:
            f.write(response.content)
        logger.info(f"Downloaded and saved as {out_file}")
        
        # Extract ZIP file
        data_files = _extract_zip_file(out_file)
        if not data_files:
            return False
        
        # Process CSV files
        success = _process_csv_files(data_files)
        
        # Cleanup temporary files
        _cleanup_temp_files()
        
        if success:
            logger.info("CJ Affiliate product fetch completed successfully")
        return success
        
    except Exception as e:
        logger.error(f"Error fetching CJ products: {e}")
        return False

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

def _process_csv_files(data_files):
    """
    Process CSV files and combine into single organized CSV
    
    Args:
        data_files: List of CSV file paths
    
    Returns:
        bool: True if successful, False otherwise
    """
    fields = ["PROGRAM_NAME", "ID", "TITLE", "LINK", "IMAGE_LINK", "AVAILABILITY", "PRICE", "SALE_PRICE"]
    
    try:
        with open(config.PRODUCTS_CSV, "w", newline='', encoding="utf-8") as outfile:
            writer = csv.DictWriter(outfile, fieldnames=fields, quoting=csv.QUOTE_MINIMAL)
            writer.writeheader()
            
            total_rows = 0
            for data_file in data_files:
                logger.info(f"Processing: {os.path.basename(data_file)}")
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
                            writer.writerow(out_row)
                            file_rows += 1
                            total_rows += 1
                    
                    logger.info(f"Processed {file_rows} rows from {os.path.basename(data_file)}")
                    
                except Exception as e:
                    logger.error(f"Error processing {data_file}: {e}")
        
        logger.info(f"Combined CSV written to: {config.PRODUCTS_CSV}")
        logger.info(f"Total rows processed: {total_rows}")
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

