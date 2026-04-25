"""
Affiliate service - handles fetching and processing affiliate product data
Sources: CJ Affiliate (GOG, GamersGate, YUPLAY, others) + IndieGala via ITAD
"""
import requests
import os
import zipfile
import csv
from datetime import datetime, timedelta
import config

from database.queries.items import (
  batch_insert_offers,
  batch_lookup_item_ids,
  batch_lookup_distributor_ids,
  delete_stale_offers,
)
from services.itad_service import fetch_indiegala_deals, fetch_gamersgate_deals, fetch_gog_deals
from utils.helpers import normalize_title, normalize_distributor_name
from utils.logger import logger


# ============================================================================
# PUBLIC API FUNCTIONS
# ============================================================================

def fetch_all_affiliate_products():
  products = build_items_info_csv()
  cleanup_temp_files()
  if products is None:
    return (False, [])
  if products:
    insert_offers_to_database(products)
  with open(config.PRODUCTS_CSV, "w", newline='', encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=FIELDS, quoting=csv.QUOTE_MINIMAL)
    writer.writeheader()
    writer.writerows(products)
  return (True, products)

def get_affiliate_products_from_csv():

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

def fetch_cj_data_files():
  # CJ HTTP credentials
  url_base = "https://datatransfer.cj.com"
  username = config.CJ_HTTP_USERNAME
  password = config.CJ_HTTP_PASSWORD
  
  if not username or not password:
    logger.error("CJ credentials not found in environment variables")
    return None
  
  today_str = datetime.now().strftime("%Y%m%d")
  yesterday_str = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
  if config.CJ_DATA_DATE == "TODAY":
    file_path = f"/datatransfer/files/7609708/outgoing/productcatalog/312045/product_feedex-shopping-{today_str}.zip"
  elif config.CJ_DATA_DATE == "YESTERDAY":
    file_path = f"/datatransfer/files/7609708/outgoing/productcatalog/312045/product_feedex-shopping-{yesterday_str}.zip"
  url = url_base + file_path
  
  # Create directories
  os.makedirs(config.TEMP_DIR, exist_ok=True)
  os.makedirs(config.CSV_DIR, exist_ok=True)
  
  try:
    response = requests.get(url, auth=(username, password))
    if response.status_code != 200:
      logger.error(f"Failed to download CJ products. Status: {response.status_code}")
      return None
    out_file = os.path.join(config.TEMP_DIR, os.path.basename(file_path))
    with open(out_file, "wb") as f:
      f.write(response.content)
    data_files = extract_zip_file(out_file)
    if not data_files:
      return None
    logger.info(f"CJ Affiliate: Fetched {len(data_files)} data files")
    return data_files
  except Exception as e:
    logger.error(f"Error fetching CJ products: {e}")
    return None

def extract_zip_file(zip_path):
  try:
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
      zip_ref.extractall(config.TEMP_DIR)
  except zipfile.BadZipFile:
    logger.error("The downloaded file is not a valid zip archive")
    return []
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
# UNIFIED PRODUCT LIST (used for both DB insert and deals matching)
# ============================================================================

FIELDS = ["PROGRAM_NAME", "ID", "TITLE", "LINK", "IMAGE_LINK", "AVAILABILITY", "PRICE", "SALE_PRICE", "DISCOUNT"]


def build_items_info_csv():
  # --- Step 1: CJ Affiliate feed ---
  cj_data_files = fetch_cj_data_files()
  if cj_data_files is None:
    logger.error("Failed to fetch CJ products")
    return None

  cj_rows = []
  for data_file in cj_data_files:
    try:
      with open(data_file, newline='', encoding="utf-8") as infile:
        reader = csv.DictReader(infile)
        for row in reader:
          out = {f: str(row.get(f, "")).replace("\n", " ").replace("\r", " ").replace("\t", " ").strip() for f in FIELDS}
          if not out.get("DISCOUNT"):
            out["DISCOUNT"] = ""
          cj_rows.append(out)
    except Exception as e:
      logger.error(f"Error processing {data_file}: {e}")

  # Separate GG and GOG rows – CJ prices for both are replaced with live
  # ITAD prices in Steps 3 & 4 below; only CJ affiliate URLs are kept.
  cj_gg = [r for r in cj_rows if r.get("PROGRAM_NAME", "").strip() == "GamersGate.com"]
  cj_gog = [r for r in cj_rows if r.get("PROGRAM_NAME", "").strip() == "GOG.COM INT"]
  cj_non_gg = [r for r in cj_rows if r.get("PROGRAM_NAME", "").strip() not in ("GamersGate.com", "GOG.COM INT")]
  logger.info(f"Step 1: CJ feed loaded ({len(cj_non_gg)} other + {len(cj_gg)} GG + {len(cj_gog)} GOG rows)")

  # --- Step 2: IndieGala via ITAD ---
  itad_indiegala = fetch_indiegala_deals(country=config.ITAD_COUNTRY, currency=config.ITAD_CURRENCY)
  indiegala_rows = [
    {f: str(p.get(f, "")).replace("\n", " ").replace("\r", " ").replace("\t", " ").strip() for f in FIELDS}
    for p in itad_indiegala
  ]
  logger.info(f"Step 2: IndieGala via ITAD ({len(indiegala_rows)} rows)")

  # --- Step 3: GamersGate via ITAD + CJ affiliate URLs ---
  # Build title→CJ-row map so we can attach the CJ affiliate link to each
  # ITAD deal (CJ link is required for affiliate commission tracking).
  gg_affiliate_map = {}
  for r in cj_gg:
    t = r.get("TITLE", "").strip()
    norm = normalize_title(t)
    if norm and norm not in gg_affiliate_map:
      gg_affiliate_map[norm] = r

  itad_gg = fetch_gamersgate_deals(country=config.ITAD_COUNTRY, currency=config.ITAD_CURRENCY)
  gg_rows = []
  for deal in itad_gg:
    title = deal["title"]
    norm = normalize_title(title)
    cj_match = gg_affiliate_map.get(norm)
    if not cj_match:
      continue  # Skip GG deals with no CJ affiliate link
    gg_rows.append({
      "PROGRAM_NAME": "GamersGate.com",
      "ID":           cj_match.get("ID", ""),
      "TITLE":        title,
      "LINK":         cj_match.get("LINK", deal["store_url"]),
      "IMAGE_LINK":   cj_match.get("IMAGE_LINK", ""),
      "AVAILABILITY": "in stock",
      "PRICE":        deal["price"],
      "SALE_PRICE":   deal["sale_price"],
      "DISCOUNT":     deal["discount"],
    })
  logger.info(f"Step 3: GamersGate via ITAD – {len(gg_rows)} matched rows "
              f"({len(itad_gg)} ITAD deals, {len(gg_affiliate_map)} CJ affiliate links)")

  # --- Step 4: GOG via ITAD + CJ affiliate URLs ---
  gog_affiliate_map = {}
  for r in cj_gog:
    t = r.get("TITLE", "").strip()
    norm = normalize_title(t)
    if norm and norm not in gog_affiliate_map:
      gog_affiliate_map[norm] = r

  itad_gog = fetch_gog_deals(country=config.ITAD_COUNTRY, currency=config.ITAD_CURRENCY)
  gog_rows = []
  for deal in itad_gog:
    title = deal["title"]
    norm = normalize_title(title)
    cj_match = gog_affiliate_map.get(norm)
    if not cj_match:
      continue  # Skip GOG deals with no CJ affiliate link
    gog_rows.append({
      "PROGRAM_NAME": "GOG.COM INT",
      "ID":           cj_match.get("ID", ""),
      "TITLE":        title,
      "LINK":         cj_match.get("LINK", deal["store_url"]),
      "IMAGE_LINK":   cj_match.get("IMAGE_LINK", ""),
      "AVAILABILITY": "in stock",
      "PRICE":        deal["price"],
      "SALE_PRICE":   deal["sale_price"],
      "DISCOUNT":     deal["discount"],
    })
  logger.info(f"Step 4: GOG via ITAD – {len(gog_rows)} matched rows "
              f"({len(itad_gog)} ITAD deals, {len(gog_affiliate_map)} CJ affiliate links)")

  # --- Step 5: Merge + fill missing discounts for CJ-only stores ---
  raw = cj_non_gg + indiegala_rows + gg_rows + gog_rows
  products = []
  for r in raw:
    price = r.get("PRICE", "").strip()
    sale = r.get("SALE_PRICE", "").strip()
    if not price or not sale:
      continue
    discount = r.get("DISCOUNT", "").strip()
    if not discount or discount == "0":
      computed = calculate_discount(price, sale)
      if computed > 0:
        r["DISCOUNT"] = str(computed)
    products.append(r)
  logger.info(f"Step 5: Merged – {len(products)} rows with valid prices")

  # --- Step 6: Write CSV ---
  os.makedirs(config.CSV_DIR, exist_ok=True)
  with open(config.PRODUCTS_CSV, "w", newline='', encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=FIELDS, quoting=csv.QUOTE_MINIMAL)
    writer.writeheader()
    writer.writerows(products)
  logger.info(f"Step 6: Wrote {config.PRODUCTS_CSV} with {len(products)} rows")
  return products

# ============================================================================
# CSV PROCESSING & DATABASE INSERTION FUNCTIONS
# ============================================================================

def insert_offers_to_database(all_rows_data):
  unique_titles = set()
  unique_program_names = set()
  for row_data in all_rows_data:
    title = row_data.get("TITLE", "").strip()
    program_name = row_data.get("PROGRAM_NAME", "").strip()
    if title:
      unique_titles.add(title)
    if program_name:
      unique_program_names.add(program_name)

  # Batch lookup via SQLModel queries
  item_id_map = batch_lookup_item_ids(unique_titles)
  normalized_to_originals = {}
  for prog_name in unique_program_names:
    norm = normalize_distributor_name(prog_name)
    if norm not in normalized_to_originals:
      normalized_to_originals[norm] = []
    normalized_to_originals[norm].append(prog_name)
  distributor_id_map = batch_lookup_distributor_ids(normalized_to_originals)

  query_values, stats = prepare_offer_inserts(all_rows_data, item_id_map, distributor_id_map)

  if query_values:
    inserted, skipped = batch_insert_offers(query_values)
    delete_stale_offers()
    logger.info(f"Inserted {inserted} offers into database")
    if skipped > 0:
      logger.warning(f"Skipped {skipped} offers with validation errors")
  else:
    logger.warning("No valid products to insert into database")

def prepare_offer_inserts(all_rows_data, item_id_map, distributor_id_map):
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

    if program_name and program_name not in distributor_stats:
      distributor_stats[program_name] = {
        'total': 0, 'valid': 0, 'missing_essential': 0, 'missing_item': 0,
        'missing_distributor': 0, 'missing_sale_price': 0,
        'price_conversion_failed': 0, 'discount_too_low': 0, 'missing_key_values': 0
      }
    if program_name:
      distributor_stats[program_name]['total'] += 1

    if not title or not program_name or not affiliate_url:
      skipped_count += 1
      missing_essential_data_count += 1
      if program_name:
        distributor_stats[program_name]['missing_essential'] += 1
      continue

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

    if not sale_price or not sale_price.strip():
      missing_sale_price_count += 1
      skipped_count += 1
      distributor_stats[program_name]['missing_sale_price'] += 1
      continue

    list_price_val = row_data.get("PRICE", "").strip()
    discount_val = row_data.get("DISCOUNT", "").strip()
    if not list_price_val:
      skipped_count += 1
      distributor_stats[program_name]['missing_key_values'] += 1
      continue

    try:
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

    if sale_price_float is None:
      skipped_count += 1
      distributor_stats[program_name]['price_conversion_failed'] += 1
      continue

    row_discount = row_data.get("DISCOUNT", "").strip()
    if row_discount and row_discount != "0":
      try:
        discount = int(float(row_discount))
      except (ValueError, TypeError):
        discount = calculate_discount(list_price_clean, sale_price_clean)
    else:
      discount = calculate_discount(list_price_clean, sale_price_clean)

    if discount < 20:
      skipped_count += 1
      distributor_stats[program_name]['discount_too_low'] += 1
      continue

    query_values.append((
      item_id,
      distributor_id,
      affiliate_url,
      image_url,
      list_price_float,
      sale_price_float,
      discount,
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

  for dist_name in ['GamersGate.com', 'GOG.COM INT', 'YUPLAY']:
    if dist_name in distributor_stats:
      stats_data = distributor_stats[dist_name]
      logger.info(f"{dist_name} stats: total={stats_data['total']}, valid={stats_data['valid']}, "
        f"missing_item={stats_data['missing_item']}, missing_distributor={stats_data['missing_distributor']}, "
        f"missing_sale_price={stats_data['missing_sale_price']}, "
        f"price_conversion_failed={stats_data['price_conversion_failed']}, "
        f"discount_too_low={stats_data['discount_too_low']}, "
        f"missing_essential={stats_data['missing_essential']}")

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
      write_missing_titles_to_file(missing_items)
  
  return query_values, stats

# ============================================================================
# HELPER/UTILITY FUNCTIONS
# ============================================================

def calculate_discount(list_price_str, sale_price_str):
  try:
    list_price_str = str(list_price_str).replace("$", "").replace(",", "").replace(" ", "").replace("USD", "").strip()
    sale_price_str = str(sale_price_str).replace("$", "").replace(",", "").replace(" ", "").replace("USD", "").strip()
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

def write_missing_titles_to_file(missing_items):
  try:
    title_info = {}
    for item in missing_items:
      title = item['title']
      program = item['program']
      if title not in title_info:
        title_info[title] = {'distributors': set(), 'count': 0}
      title_info[title]['distributors'].add(program)
      title_info[title]['count'] += 1
    sorted_titles = sorted(title_info.items(), key=lambda x: x[1]['count'], reverse=True)
    os.makedirs(config.CSV_DIR, exist_ok=True)
    with open(config.MISSING_TITLES_CSV, 'w', newline='', encoding='utf-8') as f:
      writer = csv.writer(f)
      writer.writerow(['TITLE', 'DISTRIBUTORS', 'OCCURRENCES'])
      for title, info in sorted_titles:
        distributors_str = ', '.join(sorted(info['distributors']))
        writer.writerow([title, distributors_str, info['count']])
    logger.info(f"Missing titles file created: {len(title_info)} unique titles")
  except Exception as e:
    logger.error(f"Error writing missing titles to file: {e}")

def cleanup_temp_files():
  for fname in os.listdir(config.TEMP_DIR):
    fpath = os.path.join(config.TEMP_DIR, fname)
    if os.path.isfile(fpath) and not fname.startswith("."):
      try:
        os.remove(fpath)
      except Exception:
        pass