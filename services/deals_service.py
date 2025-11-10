"""
Deals matching service
Matches affiliate products with Steam top sellers
"""
import csv
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import config
from utils.logger import logger

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
            # Skip header row if present
            if product_list and product_list[0][0] == "PROGRAM_NAME":
                product_list = product_list[1:]
        
        # Read Steam file
        with open(config.STEAMDB_CSV, encoding="utf-8") as sf:
            steam_reader = csv.reader(sf)
            steam_list = list(steam_reader)
            # Skip header row if present
            if steam_list and steam_list[0][0].lower() == "name":
                steam_list = steam_list[1:]
        
        deals = []
        
        # Match products with Steam top sellers
        # CSV columns: PROGRAM_NAME, ID, TITLE, LINK, IMAGE_LINK, AVAILABILITY, PRICE, SALE_PRICE, DISCOUNT
        for s_row in steam_list:
            target_title = s_row[0]
            for p_row in product_list:
                if len(p_row) >= 6 and target_title == p_row[2] and (p_row[5] == "in stock" or p_row[5] == "in_stock"):
                    game_obj = {
                        "source": p_row[0],
                        "title": p_row[2],
                        "link": p_row[3],
                        "image_link": p_row[4],
                    }
                    # Add discount if available (IndieGala products have it)
                    if len(p_row) >= 9 and p_row[8]:
                        game_obj["DISCOUNT"] = p_row[8]
                    # Add price fields for validation
                    if len(p_row) >= 8:
                        if p_row[7]:  # SALE_PRICE
                            game_obj["SALE_PRICE"] = p_row[7]
                        if p_row[6]:  # PRICE
                            game_obj["PRICE"] = p_row[6]
                    deals.append(game_obj)
        
        logger.info(f"Found {len(deals)} matching deals")
        return deals
        
    except FileNotFoundError as e:
        logger.error(f"File not found: {e}")
        return []
    except Exception as e:
        logger.error(f"Error finding deals: {e}")
        return []


