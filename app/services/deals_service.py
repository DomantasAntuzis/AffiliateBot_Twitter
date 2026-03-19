"""
Deals matching service
Matches affiliate products with Steam top sellers
"""
import csv
import sys
import os
import re
import random
import json

import config
from utils.logger import logger
from utils.helpers import normalize_title
from database.queries.steam import get_steam_topsellers


def find_matching_deals(products_list=None):
    logger.info("Finding deals by matching affiliate products with Steam top sellers")

    steam_games = get_steam_topsellers()

    if products_list is not None:
        product_list = products_list
    else:
        try:
            with open(config.PRODUCTS_CSV, encoding="utf-8") as pf:
                reader = csv.DictReader(pf)
                product_list = list(reader)
        except Exception as e:
            logger.error(f"Error reading products CSV: {e}")
            return []

    try:
        deals = []
        for s_row in steam_games:
            steam_title = s_row[0]
            steam_price = float(s_row[1])
            norm_steam = normalize_title(steam_title)

            for p in product_list:
                distributor = p.get("PROGRAM_NAME", "").strip()
                title = p.get("TITLE", "").strip()
                link = p.get("LINK", "").strip()
                image_link = p.get("IMAGE_LINK", "").strip()
                availability = p.get("AVAILABILITY", "").strip()
                price = p.get("PRICE", "").strip()
                sale_price = p.get("SALE_PRICE", "").strip()
                discount = p.get("DISCOUNT", "").strip()

                if not price or not sale_price:
                    continue
                if normalize_title(title) != norm_steam:
                    continue
                if availability not in ("in stock", "in_stock"):
                    continue

                try:
                    price_float = _parse_price(price)
                    sale_price_float = _parse_price(sale_price)
                    deal_price = sale_price_float
                    if discount and discount != "0":
                        discount_int = int(round(float(discount)))
                    elif price_float > 0:
                        discount_int = round(((price_float - sale_price_float) / price_float) * 100)
                    else:
                        discount_int = 0
                except (ValueError, TypeError):
                    continue
                if price_float <= 0 or sale_price_float <= 0:
                    continue
                if steam_price <= deal_price:
                    continue

                deals.append({
                    "source": distributor,
                    "title": title,
                    "link": link,
                    "image_link": image_link,
                    "price": price_float,
                    "salePrice": sale_price_float,
                    "discount": discount_int,
                })
        
        logger.info(f"Found {len(deals)} matching deals")


        sorted_deals = []
        different_sources = []
        for deal in deals:
            if deal["source"] not in different_sources:
                different_sources.append(deal["source"])

        for source in different_sources:
            source_deals = [deal for deal in deals if deal["source"] == source]
            sorted_deals.append(source_deals)

        #shuffle each group
        for group in sorted_deals:
            random.shuffle(group)

        with open("sorted_deals.json", "w", encoding="utf-8") as f:
            json.dump(sorted_deals, f, indent=4)

        return sorted_deals

    except Exception as e:
        logger.error(f"Error finding deals: {e}")
        return []

def _get_deal_price(p_row, distributor):
    # CSV columns: PROGRAM_NAME, ID, TITLE, LINK, IMAGE_LINK, AVAILABILITY, PRICE, SALE_PRICE, DISCOUNT
    if distributor == "GamersGate.com":
        return p_row[6]  # Use PRICE
    else:
        return p_row[7]  # Use SALE_PRICE


def _parse_price(price_str):
    try:
        # Remove currency symbols and text, keep only numbers and decimal point
        price_clean = re.sub(r'[^\d.]', '', str(price_str))
        return float(price_clean)
    except:
        return 0.0

