from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import json
import csv
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

steamdbFile = "csv_data/steamdb_results.csv"
SFile = open(steamdbFile, encoding="utf-8")
SFileReader = csv.reader(SFile)
SFileList = list(SFileReader)
SFile.close()

def comparePrices(title, price):
	for s_row in SFileList:
		if title == s_row[0] and s_row[1] != "Free":
			steamdb_price = float(s_row[1].replace('$', '').replace(',', ''))
			if price < steamdb_price:
				print(f"CHEAPER: ${price:.2f} vs Steam ${steamdb_price:.2f}")
				return True
			else:
				print(f"MORE EXPENSIVE: ${price:.2f} vs Steam ${steamdb_price:.2f}")
				return False

#klaidingos kainos ir nuolaidos

def validate_deals(deal, driver):
	"""Returns: modified deal object if valid, None if invalid"""
	deal_title = deal["title"]
	deal_link = deal["link"]
	deal_source = deal["source"]
	
	try:
		driver.get(deal_link)
		print(f"{deal_source}: {deal_title} - {deal_link}")
		
		#Skip validating same source same name deals
		skip_GOG_dupes = []
		skip_GamersGate_dupes = []
		skip_YUPLAY_dupes = []

		if deal_source == "GOG.COM INT" and deal_title not in skip_GOG_dupes:

			wait = WebDriverWait(driver, 15)
			discount_span = wait.until(EC.presence_of_element_located((By.CLASS_NAME, "product-actions-price__discount")))
			discounted_price_span = wait.until(EC.presence_of_element_located((By.CLASS_NAME, "product-actions-price__final-amount")))
			
			discount_text = driver.execute_script("return arguments[0].textContent;", discount_span).strip().replace('%', '').replace('-', '')
			discount = int(discount_text)
			discounted_price_text = driver.execute_script("return arguments[0].textContent;", discounted_price_span).strip()
			discounted_price = float(discounted_price_text)

			print(f"Found: {discount}% off - ${discounted_price:.2f}")
			cheaper = comparePrices(deal_title, discounted_price)
			
			if discount and discounted_price and cheaper:
				deal["discount"] = discount
				deal["salePrice"] = f"${discounted_price:.2f}"
				skip_GOG_dupes.append(deal_title)
				print("VALID - Cheaper than Steam")
				return deal  # Return modified deal
			else:
				skip_GOG_dupes.append(deal_title)
				print("INVALID - More expensive than Steam")
				return None  # Invalid
				
		elif deal_source == "GamersGate.com":
			wait = WebDriverWait(driver, 15)

			try:
			# 1. Click the age dropdown
				dropdown_year = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, '.select[data-name="age_year"]')))
				dropdown_year.click()

				year_option = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, '.select[data-name="age_year"] a[data-value="2003"]')))
				year_option.click()

				# 2. Click the month dropdown
				dropdown_month = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, '.select[data-name="age_month"]')))
				dropdown_month.click()

				month_option = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, '.select[data-name="age_month"] a[data-value="1"]')))
				month_option.click()

				#3. Click the day dropdown
				dropdown_day = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, '.select[data-name="age_day"]')))
				dropdown_day.click()

				day_option = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, '.select[data-name="age_day"] a[data-value="1"]')))
				day_option.click()

				#4. Click the submit button
				submit_button = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, 'button[type="submit"]')))
				submit_button.click()
			except Exception:
				# print("GamersGate age vertification menu not found")
				pass
		
			discount_element = wait.until(EC.presence_of_element_located((By.CLASS_NAME, "catalog-item--discount-value")))
			discount = int(discount_element.text.strip().replace('%', '').replace('-', ''))
			
			discounted_price_element = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".catalog-item--price span")))
			discounted_price_text = driver.execute_script("return arguments[0].textContent;", discounted_price_element).strip()
			price_numeric_text = discounted_price_text.replace('$', '').replace(',', '').strip()
			discounted_price = float(price_numeric_text)
			
			image_src = driver.find_element(By.CSS_SELECTOR, "div.catalog-item--image img").get_attribute("src")

			print(f"Found: {discount}% off - ${discounted_price:.2f}")
			cheaper = comparePrices(deal_title, discounted_price)
			
			if discount > 0 and discounted_price and cheaper:
				deal["discount"] = discount
				deal["salePrice"] = f"${discounted_price:.2f}"
				deal["image_link"] = image_src
				skip_GamersGate_dupes.append(deal_title)
				print("VALID - Cheaper than Steam")
				return deal  # Return modified deal
			else:
				skip_GamersGate_dupes.append(deal_title)
				print("INVALID - More expensive than Steam")
				return None  # Invalid

		elif deal_source == "YUPLAY" and deal_title not in skip_YUPLAY_dupes:
			wait = WebDriverWait(driver, 15)
			
			product_container = wait.until(EC.presence_of_element_located((By.CLASS_NAME, "product-second-container")))
			discount_element = product_container.find_element(By.CLASS_NAME, "catalog-item-discount-label")
			discounted_price = product_container.find_element(By.CLASS_NAME, "catalog-item-sale-price")

			if discount_element and discounted_price:
				discount_span = discount_element.find_elements(By.TAG_NAME, "span")
				
				if discount_span:
					discount = driver.execute_script("return arguments[0].textContent;", discount_span[0]).strip()
					discount_int = int(discount)
					price = driver.execute_script("return arguments[0].textContent;", discounted_price).replace('$', '').replace(',', '').strip()
					priceFloat = float(price)

					print(f"YUPLAY: {discount_int}% off - ${priceFloat:.2f}")
					cheaper = comparePrices(deal_title, priceFloat)

					if discount_int > 0 and priceFloat and cheaper:
						deal["discount"] = discount_int
						deal["salePrice"] = f"${priceFloat:.2f}"
						skip_YUPLAY_dupes.append(deal_title)
						print("VALID DEAL")
						return deal  # Return modified deal
					else:
						print("INVALID: Not cheaper than Steam")
						skip_YUPLAY_dupes.append(deal_title)
						return None  # Invalid
				else:
					print("INVALID: No discount found")
					skip_YUPLAY_dupes.append(deal_title)
					return None  # Invalid
			else:
				print("INVALID: Price elements not found")
				skip_YUPLAY_dupes.append(deal_title)
				return None
		else:
			print("INVALID - Unsupported source")
			return None	
				
	except Exception as e:
		print(f"ERROR - Missing elements: {e}")
		return None
	
def create_chrome_driver():
	"""Create a Chrome driver with optimized settings"""
	rotating_proxy = "http://p.webshare.io:9999"
	
	chrome_options = Options()
	chrome_options.add_argument("--headless")
	chrome_options.add_argument("--disable-gpu")
	chrome_options.add_argument("--window-size=1920,1080")
	chrome_options.add_argument("--no-sandbox")
	chrome_options.add_argument("--disable-blink-features=AutomationControlled")
	chrome_options.add_argument(f"--proxy-server={rotating_proxy}")
	
	# Block specific resource types and URL patterns
	chrome_options.add_argument("--disable-images")
	chrome_options.add_argument("--disable-plugins")
	chrome_options.add_argument("--disable-extensions")
	chrome_options.add_argument("--disable-dev-shm-usage")
	
	# Enable request interception for blocking specific URLs
	chrome_options.add_argument("--enable-features=NetworkService")
	chrome_options.add_argument("--disable-features=VizDisplayCompositor")
	chrome_options.page_load_strategy = 'eager'
	
	driver = webdriver.Chrome(options=chrome_options)
	
	# Enable request interception and block specific URL patterns
	driver.execute_cdp_cmd('Network.enable', {})
	driver.execute_cdp_cmd('Network.setBlockedURLs', {
		'urls': [
			'*.jpg',
			'*.jpeg', 
			'*.png',
			'*.gif',
			'*.css',
			'*.svg',
			'*.woff',
			'*.woff2',
			'*.ttf',
			'*.eot',
			'*/fonts/*',
			'*font*',
			'*analytics*',
			'*tracking*',
			'*ads*'
		]
	})
	
	return driver

def validate_deal_worker(deal, posted_games_list):
	"""Worker function to validate a single deal with its own driver"""
	driver = None
	try:
		# print(f"[Thread {threading.current_thread().name}] Processing: {deal['title']}")
		
		if deal["title"] in posted_games_list:
			# print(f"[Thread {threading.current_thread().name}] Skipping {deal['title']} - already posted")
			return None
		
		# Create driver for this worker
		driver = create_chrome_driver()
		
		# Validate the deal
		result = validate_deals(deal, driver)
		
		# if result:
		#	print(f"[Thread {threading.current_thread().name}] ✓ Valid: {deal['title']}")
		# else:
		#	print(f"[Thread {threading.current_thread().name}] ✗ Invalid: {deal['title']}")
		
		return result
		
	except Exception as e:
		print(f"[Thread {threading.current_thread().name}] ERROR processing {deal['title']}: {e}")
		return None
	finally:
		if driver:
			try:
				driver.quit()
			except Exception:
				pass

def validate_deals_export(deals):
	"""Parallel validation of deals using 3 concurrent browser instances"""
	deals_data = list(deals)
	
	# Load posted games once
	with open("posted_games.txt", encoding="utf-8") as f:
		posted_games_list = f.read().splitlines()
	
	valid_deals = []
	
	# Use ThreadPoolExecutor with 3 workers for parallel processing
	# print(f"Starting parallel validation of {len(deals_data)} deals with 3 workers...")
	
	with ThreadPoolExecutor(max_workers=3, thread_name_prefix="DealValidator") as executor:
		# Submit all deals for processing
		future_to_deal = {
			executor.submit(validate_deal_worker, deal, posted_games_list): deal 
			for deal in deals_data
		}
		
		# Collect results as they complete
		completed = 0
		for future in as_completed(future_to_deal):
			deal = future_to_deal[future]
			completed += 1
			
			try:
				result = future.result()
				if result:
					valid_deals.append(result)
				# print(f"Progress: {completed}/{len(deals_data)} deals processed")
				
			except Exception as e:
				print(f"Deal {deal['title']} generated an exception: {e}")
	
	print(f"\nFound {len(valid_deals)} valid deals")
	
	# Post-processing (deduplication and sorting)
	#remove duplicates
	deal_counts = {}
	for deal in valid_deals:
			deal_key = f"{deal['title']}_{deal['source']}"
			deal_counts[deal_key] = deal_counts.get(deal_key, 0) + 1

	# Second pass: keep only deals that appear exactly once
	non_duplicate_deals = []
	for deal in valid_deals:
			deal_key = f"{deal['title']}_{deal['source']}"
			if deal_counts[deal_key] == 1:  # Only keep if it appears exactly once
					non_duplicate_deals.append(deal)

	sorted_deals = []

	#sort by source
	different_sources = []
	for deal in non_duplicate_deals:
		if deal["source"] not in different_sources:
			different_sources.append(deal["source"])

	for source in different_sources:
		source_deals = []
		for deal in non_duplicate_deals:
			if deal["source"] == source:
				source_deals.append(deal)
		sorted_deals.append(source_deals)

	valid_deals_file = "valid_deals.json"
	with open(valid_deals_file, "w", encoding="utf-8") as f:
		json.dump(sorted_deals, f, indent=4, ensure_ascii=False)
	
	# print(f"Results saved to {valid_deals_file}")

