from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import json
import csv

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
		
		#Skip validating same source same name deals
		skip_GOG_dupes = []
		skip_YUPLAY_dupes = []

		if deal_source == "GOG.COM INT" and deal_title not in skip_GOG_dupes:

			wait = WebDriverWait(driver, 5)
			discount_span = wait.until(EC.presence_of_element_located((By.CLASS_NAME, "product-actions-price__discount")))
			discounted_price_span = wait.until(EC.presence_of_element_located((By.CLASS_NAME, "product-actions-price__final-amount")))
			
			discount_text = driver.execute_script("return arguments[0].textContent;", discount_span).strip().replace('%', '').replace('-', '')
			discount = int(discount_text)
			discounted_price_text = driver.execute_script("return arguments[0].textContent;", discounted_price_span).strip()
			discounted_price = float(discounted_price_text)

			print(f"GOG: {discount}% off - ${discounted_price:.2f}")
			cheaper = comparePrices(deal_title, discounted_price)
			
			if discount and discounted_price and cheaper:
				deal["discount"] = discount
				deal["salePrice"] = f"${discounted_price:.2f}"
				skip_GOG_dupes.append(deal_title)
				print("VALID DEAL")
				return deal  # Return modified deal
			else:
				skip_GOG_dupes.append(deal_title)
				print("INVALID: Not cheaper than Steam")
				return None  # Invalid
				
		elif deal_source == "YUPLAY" and deal_title not in skip_YUPLAY_dupes:
			wait = WebDriverWait(driver, 5)
			
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
			print("INVALID: Unsupported source")
			return None	
				
	except Exception as e:
		print(f"ERROR: {e}")
		return None
	
def validate_deals_export(deals):
	rotating_proxy = "http://p.webshare.io:9999"

	# Initialize browser ONCE outside the function
	chrome_options = Options()
	chrome_options.add_argument("--headless")
	chrome_options.add_argument("--disable-gpu")
	chrome_options.add_argument("--window-size=1920,1080")
	chrome_options.add_argument("--no-sandbox")
	chrome_options.add_argument("--disable-blink-features=AutomationControlled")
	chrome_options.add_argument(f"--proxy-server={rotating_proxy}")

	#Copied from stackoverflow
	#idk what most of these do
	#but it works
	
	prefs = {'profile.default_content_setting_values': {'images': 2, 
															'plugins': 2, 'popups': 2, 'geolocation': 2, 
															'notifications': 2, 'auto_select_certificate': 2, 'fullscreen': 2, 
															'mouselock': 2, 'mixed_script': 2, 'media_stream': 2, 
															'media_stream_mic': 2, 'media_stream_camera': 2, 'protocol_handlers': 2, 
															'ppapi_broker': 2, 'automatic_downloads': 2, 'midi_sysex': 2, 
															'push_messaging': 2, 'ssl_cert_decisions': 2, 'metro_switch_to_desktop': 2, 
															'protected_media_identifier': 2, 'app_banner': 2, 'site_engagement': 2, 
															'durable_storage': 2}}
	
	chrome_options.add_experimental_option('prefs', prefs)

	#minimize proxy requests
	chrome_options.add_argument("--blink-settings=imagesEnabled=false")
	chrome_options.page_load_strategy = 'eager'

	# Create browser instance once
	# driver = webdriver.Chrome(options=chrome_options)

	# minimize proxy requests
	# caps = DesiredCapabilities().CHROME
	# caps["pageLoadStrategy"] = "eager"
	driver = webdriver.Chrome(options=chrome_options)

	try:
		# Process all deals using the same browser
		deals_data = list(deals)

		with open("posted_games.txt", encoding="utf-8") as f:
			posted_games_list = f.read().splitlines()

		valid_deals = []  # Store valid deals

		for deal in deals_data:
			print(f"deal: {deal['title']}")
			if deal["title"] in posted_games_list:
				print(f"skipping {deal['title']} because it's already posted")
				continue
			
			result = validate_deals(deal, driver)
			
			if result:
				valid_deals.append(result)
			
		print(f"\nFound {len(valid_deals)} valid deals")

	finally:

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

		# Close browser only at the very end
		driver.quit()

