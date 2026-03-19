"""
Affiliate service - handles fetching and processing affiliate product data
Supports CJ Affiliate API and IndieGala web scraping
"""
import requests
import os
import zipfile
import csv
import time
from datetime import datetime, timedelta
from seleniumwire import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import html
import config

from database.queries.items import (
  batch_insert_offers,
  batch_lookup_item_ids,
  batch_lookup_distributor_ids,
)
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

def fetch_gamersgate_page(session, page=1, platform="pc", timestamp=None):
  if timestamp is None:
    timestamp = int(time.time() * 1000)
  
  url = "https://www.gamersgate.com/api/offers/"
  params = {
  "platform": platform,
  "timestamp": timestamp,
  "need_change_browser_url": "true",
  "activations": 1
  }
  
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

def parse_gamersgate_item(item):
  
  # Clean prices (remove HTML entities and currency symbols)
  baseprice = item.get("baseprice", "")
  if baseprice:
    baseprice = html.unescape(baseprice)
    baseprice = baseprice.replace("&nbsp;", " ").replace("€", "").replace("$", "").replace("£", "").replace("¥", "").strip()
  raw_price = item.get("raw_price", "").strip()
  if raw_price:
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
  try:
    logger.info("Fetching GamersGate offers from API...")
    proxy_url = config.PROXY_URL
    session = requests.Session()
    session.proxies = {"http": proxy_url, "https": proxy_url}

    all_gamersgate_offers = []
    platform = "pc"
    page = 1
    previous_page_names = None
    session_timestamp = int(time.time() * 1000)
    consecutive_failures = 0
    max_consecutive_failures = 3

    while True:
      data = fetch_gamersgate_page(session=session, page=page, platform=platform, timestamp=session_timestamp)
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
      parsed_items = [parse_gamersgate_item(i) for i in catalog]
      current_names = [item.get("name") for item in parsed_items]
      if previous_page_names is not None:
        current_sample = current_names[:5] if len(current_names) >= 5 else current_names
        previous_sample = previous_page_names[:5] if len(previous_page_names) >= 5 else previous_page_names
        if current_sample == previous_sample:
          break
      all_gamersgate_offers.extend(parsed_items)
      previous_page_names = current_names
      consecutive_failures = 0
      page += 1
      time.sleep(1.5)

    session.close()
    logger.info(f"Collected {len(all_gamersgate_offers)} GamersGate offers from {page-1} pages")

    affiliate_products = get_affiliate_products_from_csv()
    if not affiliate_products:
      logger.error("No affiliate products found in CSV")
      return False

    affiliate_map = {}
    for product in affiliate_products:
      title = product.get("TITLE", "").strip()
      program_name = product.get("PROGRAM_NAME", "").strip()
      if title and program_name == "GamersGate.com" and normalize_title(title) not in affiliate_map:
        affiliate_map[normalize_title(title)] = product

    logger.info(f"Found {len(affiliate_map)} GamersGate affiliate products in CSV")
  
    matched_offers = []
    for gg_offer in all_gamersgate_offers:
      gg_title = gg_offer.get("name", "").strip()
      if not gg_title or not gg_offer.get("is_available") or not gg_offer.get("raw_price", "").strip():
        continue
      affiliate_product = affiliate_map.get(normalize_title(gg_title))
      if not affiliate_product:
        continue
      sale_price = gg_offer.get("raw_price", "").strip()
      baseprice = gg_offer.get("baseprice", "").strip()
      discount_percent = gg_offer.get("discount_percent", 0)
      if not baseprice and discount_percent > 0 and sale_price:
        try:
          sale_float = float(sale_price.replace("$", "").replace(",", "").strip())
          baseprice = str(round(sale_float / (1 - discount_percent / 100), 2))
        except Exception:
          pass
      if baseprice and sale_price:
        matched_offers.append({
          "TITLE": gg_title,
          "PROGRAM_NAME": "GamersGate.com",
          "LINK": affiliate_product.get("LINK", "").strip(),
          "IMAGE_LINK": affiliate_product.get("IMAGE_LINK", "").strip(),
          "PRICE": baseprice,
          "SALE_PRICE": sale_price,
          "DISCOUNT": str(discount_percent)
        })

    logger.info(f"Matched {len(matched_offers)} GamersGate offers with affiliate products")
    if not matched_offers:
      logger.warning("No matched GamersGate offers to insert")
      return True
    try:
      insert_offers_to_database(matched_offers)
      logger.info("Successfully processed GamersGate offers")
      return True
    except Exception as e:
      logger.error(f"Error inserting GamersGate offers: {e}")
      return False
  except Exception as e:
    logger.error(f"Error in insert_gamersgate_offers: {e}")
    return False


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
# INDIEGALA SCRAPING FUNCTIONS
# ============================================================================

def fetch_indiegala_data():
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
  chrome_options.add_argument("--headless=new")
  chrome_options.add_argument("--disable-gpu")
  chrome_options.add_argument("--window-size=1920,1080")
  chrome_options.add_argument("--no-sandbox")
  chrome_options.add_argument("--disable-blink-features=AutomationControlled")

  proxy_url = config.PROXY_URL
  if proxy_url:
    sw_options = {
      "proxy": {"http": proxy_url, "https": proxy_url, "no_proxy": "localhost,127.0.0.1"},
      "verify_ssl": False,
    }
    driver = webdriver.Chrome(
      service=Service(ChromeDriverManager().install()),
      seleniumwire_options=sw_options,
      options=chrome_options,
    )
  
  # Set timeouts for faster page loads
  driver.set_page_load_timeout(20)  # Max 20 seconds per page
  driver.implicitly_wait(2)  # Reduced implicit wait
  
  try:
    driver.get(url)
    wait = WebDriverWait(driver, 5)
    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".relative.main-list-results-item")))
    game_products = []
    seen_titles = set()
    duplicate_count = 0
    next_nr = 2
    consecutive_failures = 0
    max_failures = 3
    while True:
      game_cards = driver.find_elements(By.CSS_SELECTOR, ".relative.main-list-results-item")
      if not game_cards:
        logger.warning("No game cards found on page, stopping")
        break
      for game_card in game_cards:
        try:
          game_title = game_card.find_element(By.CSS_SELECTOR, "h3.bg-gradient-red").text
          if game_title in seen_titles:
            duplicate_count += 1
            continue
          seen_titles.add(game_title)
          game_discount = game_card.find_element(By.CSS_SELECTOR, "div.main-list-results-item-discount").text.replace("%", "").replace("-", "")
          game_price = game_card.find_element(By.CSS_SELECTOR, "div.main-list-results-item-price-new").text
          game_link = game_card.find_element(By.CSS_SELECTOR, "figure.relative a").get_attribute("href")
          game_affiliate_link = game_link + '?ref=mzvkywq'
          try:
            game_image = game_card.find_element(By.CSS_SELECTOR, "figure.relative img.async-img-load.display-none").get_attribute("src")
          except Exception:
            game_image = ""
          original_price = ""
          try:
            original_price_elem = game_card.find_element(By.CSS_SELECTOR, "div.main-list-results-item-price-old")
            original_price = original_price_elem.text.replace(" ", "")
          except Exception:
            pass
          try:
            discount_percent = int(game_discount) if game_discount else 0
          except Exception:
            discount_percent = 0
          product = {
            "PROGRAM_NAME": "IndieGala",
            "ID": f"IG-{game_title.replace(' ', '-').replace(':', '')[:50]}",
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
      try:
        next_button = None
        selectors = [f"a[onclick*='/{next_nr}']", f"a[href*='/{next_nr}']", f"a:contains('{next_nr}')"]
        for selector in selectors:
          try:
            next_button = driver.find_element(By.CSS_SELECTOR, selector)
            if next_button and next_button.is_displayed():
              break
          except Exception:
            continue
        if next_button and next_button.is_displayed():
          driver.execute_script("arguments[0].click();", next_button)
          next_nr += 1
          consecutive_failures = 0
          try:
            WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.CSS_SELECTOR, ".relative.main-list-results-item")))
            time.sleep(0.5)
          except Exception:
            break
        else:
          break
      except Exception as e:
        consecutive_failures += 1
        logger.warning(f"Error navigating to page {next_nr}: {e}")
        if consecutive_failures >= max_failures:
          break
    logger.info(f"IndieGala: Fetched {len(game_products)} products")
    return game_products
  except Exception as e:
    logger.error(f"Error scraping IndieGala: {e}")
    return None
  finally:
    driver.quit()


# ============================================================================
# UNIFIED PRODUCT LIST (used for both DB insert and deals matching)
# ============================================================================

FIELDS = ["PROGRAM_NAME", "ID", "TITLE", "LINK", "IMAGE_LINK", "AVAILABILITY", "PRICE", "SALE_PRICE", "DISCOUNT"]


def build_items_info_csv():
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

  cj_non_gg = [r for r in cj_rows if r.get("PROGRAM_NAME", "").strip() != "GamersGate.com"]
  cj_gg = [r for r in cj_rows if r.get("PROGRAM_NAME", "").strip() == "GamersGate.com"]
  logger.info(f"Step 1: CJ base loaded ({len(cj_non_gg)} non-GG, {len(cj_gg)} GG)")

  indiegala_products = fetch_indiegala_data()
  if indiegala_products is None:
    indiegala_products = []
  indiegala_rows = []
  for p in indiegala_products:
    indiegala_rows.append({f: str(p.get(f, "")).replace("\n", " ").replace("\r", " ").replace("\t", " ").strip() for f in FIELDS})
  logger.info(f"Step 2: IndieGala ADDED ({len(indiegala_rows)} rows)")

  affiliate_map = {}
  for p in cj_gg:
    t = p.get("TITLE", "").strip()
    if t and normalize_title(t) not in affiliate_map:
      affiliate_map[normalize_title(t)] = p
  matched_gg = fetch_and_match_gamersgate_offers(affiliate_map)
  scraped_titles = {normalize_title(m["TITLE"]) for m in matched_gg}
  cj_gg_keep = [r for r in cj_gg if normalize_title(r.get("TITLE", "")) not in scraped_titles]
  logger.info(f"Step 3: GamersGate REPLACED (matched {len(matched_gg)}, kept {len(cj_gg_keep)} fallback)")

  raw = cj_non_gg + indiegala_rows + cj_gg_keep + matched_gg
  products = []
  for r in raw:
    price = r.get("PRICE", "").strip()
    sale = r.get("SALE_PRICE", "").strip()
    discount = r.get("DISCOUNT", "").strip()
    if not price or not sale:
      continue
    if not discount or discount == "0":
      discount = str(calculate_discount(price, sale))
      r["DISCOUNT"] = discount
    if discount:
      products.append(r)
  logger.info(f"Step 4: Discounts calculated/applied where missing, {len(products)} rows ready")

  os.makedirs(config.CSV_DIR, exist_ok=True)
  with open(config.PRODUCTS_CSV, "w", newline='', encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=FIELDS, quoting=csv.QUOTE_MINIMAL)
    writer.writeheader()
    writer.writerows(products)
  logger.info(f"Step 5: Wrote {config.PRODUCTS_CSV} with {len(products)} rows")
  return products

def fetch_and_match_gamersgate_offers(affiliate_map):
  try:
    proxy_url = config.PROXY_URL
    session = requests.Session()
    session.proxies = {"http": proxy_url, "https": proxy_url}

    all_gg = []
    page = 1
    prev_names = None
    ts = int(time.time() * 1000)
    fails = 0
    while True:
      data = fetch_gamersgate_page(session=session, page=page, platform="pc", timestamp=ts)
      if not data:
        fails += 1
        if fails >= 3:
          break
        page += 1
        time.sleep(1)
        continue
      cat = data.get("catalog", [])
      if not cat:
        break
      parsed = [parse_gamersgate_item(i) for i in cat]
      names = [x.get("name") for x in parsed]
      if prev_names:
        s1 = names[:5] if len(names) >= 5 else names
        s2 = prev_names[:5] if len(prev_names) >= 5 else prev_names
        if s1 == s2:
          break
      all_gg.extend(parsed)
      prev_names = names
      fails = 0
      page += 1
      time.sleep(1.5)
    session.close()

    matched = []
    for gg in all_gg:
      title = gg.get("name", "").strip()
      if not title or not gg.get("is_available") or not gg.get("raw_price", "").strip():
        continue
      aff = affiliate_map.get(normalize_title(title))
      if not aff:
        continue
      sp = gg.get("raw_price", "").strip()
      bp = gg.get("baseprice", "").strip()
      d = gg.get("discount_percent", 0)
      if not bp and d > 0 and sp:
        try:
          bp = str(round(float(sp.replace("$", "").replace(",", "").strip()) / (1 - d / 100), 2))
        except Exception:
          pass
      if bp and sp:
        matched.append({
          "PROGRAM_NAME": "GamersGate.com",
          "ID": aff.get("ID", ""),
          "TITLE": title,
          "LINK": aff.get("LINK", "").strip(),
          "IMAGE_LINK": aff.get("IMAGE_LINK", "").strip(),
          "AVAILABILITY": "in stock",
          "PRICE": bp,
          "SALE_PRICE": sp,
          "DISCOUNT": str(d)
        })
    logger.info(f"GamersGate: matched {len(matched)} offers")
    return matched
  except Exception as e:
    logger.error(f"Error fetching/matching GamersGate: {e}")
    return []


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
    count = batch_insert_offers(query_values)
    logger.info(f"Inserted {count} offers into database")
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

    is_valid = 1
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