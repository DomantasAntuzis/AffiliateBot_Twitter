from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import json
import time
import csv
import re

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
				print(f"{title} is cheaper than on steam: {price} < {steamdb_price}")
				return True
			else:
				print(f"{title} is more expensive than on steam: {price} > {steamdb_price}")
				return False

#klaidingos kainos ir nuolaidos

def validate_deals(deal, driver):
	"""Returns: modified deal object if valid, None if invalid"""
	deal_title = deal["title"]
	deal_link = deal["link"]
	deal_source = deal["source"]
	
	try:
		driver.get(deal_link)
		
		if deal_source == "GOG.COM INT":
			wait = WebDriverWait(driver, 10)
			discount_span = wait.until(EC.presence_of_element_located((By.CLASS_NAME, "product-actions-price__discount")))
			discounted_price_span = wait.until(EC.presence_of_element_located((By.CLASS_NAME, "product-actions-price__final-amount")))
			
			discount_text = driver.execute_script("return arguments[0].textContent;", discount_span).strip().replace('%', '').replace('-', '')
			discount = int(discount_text)
			discounted_price_text = driver.execute_script("return arguments[0].textContent;", discounted_price_span).strip()
			discounted_price = float(discounted_price_text)

			print(f"discount: {discount} discounted_price: {discounted_price}")
			cheaper = comparePrices(deal_title, discounted_price)
			
			if discount and discounted_price and cheaper:
				deal["discount"] = discount
				deal["salePrice"] = discounted_price
				return deal  # Return modified deal
			else:
				return None  # Invalid
				
		elif deal_source == "YUPLAY":
			wait = WebDriverWait(driver, 10)
			
			product_container = wait.until(EC.presence_of_element_located((By.CLASS_NAME, "product-second-container")))
			discount_element = product_container.find_element(By.CLASS_NAME, "catalog-item-discount-label")
			discounted_price = product_container.find_element(By.CLASS_NAME, "catalog-item-sale-price")

			if discount_element and discounted_price:
				discount_span = discount_element.find_elements(By.TAG_NAME, "span")
				
				if discount_span:
					discount_number = driver.execute_script("return arguments[0].textContent;", discount_span[0]).strip()
					price = driver.execute_script("return arguments[0].textContent;", discounted_price).replace('$', '').replace(',', '').strip()
					priceFloat = float(price)

					print(f"discount_element: {discount_number} discounted_price: {priceFloat}")
					cheaper = comparePrices(deal_title, priceFloat)

					if discount_number.isdigit() and int(discount_number) > 0 and priceFloat and cheaper:
						deal["discount"] = discount_number
						deal["salePrice"] = priceFloat
						return deal  # Return modified deal
					else:
						return None  # Invalid
				else:
					return None  # Invalid
			else:
				return None  # Invalid
				
	except Exception as e:
		print(f"Error processing {deal_title}: {e}")
		return None
	
	return None

# Initialize browser ONCE outside the function
chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--window-size=1920,1080")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-blink-features=AutomationControlled")

# Create browser instance once
driver = webdriver.Chrome(options=chrome_options)

try:
	# Process all deals using the same browser
	dealsFile = "deals.json"
	with open(dealsFile, encoding="utf-8") as f:
		dealsFileReader = json.load(f)
		dealsFileList = list(dealsFileReader)

	with open("posted_games.txt", encoding="utf-8") as f:
		posted_games_list = f.read().splitlines()
		print(f"posted_games_list: {posted_games_list}")

	valid_deals = []  # Store valid deals

	for deal in dealsFileList:
		print(f"deal: {deal['title']}")
		if deal["title"] in posted_games_list:
			print(f"skipping {deal['title']} because it's already posted")
			continue
		
		result = validate_deals(deal, driver)
		
		if result:
			print(f"✅ Valid deal: {deal['title']}")
			valid_deals.append(result)
		else:
			print(f"❌ Invalid deal: {deal['title']} - No discount or price not cheaper")
		
	print(f"\nFound {len(valid_deals)} valid deals")

finally:

	#remove duplicates
	deal_counts = {}
	for deal in valid_deals:
			deal_key = f"{deal['title']}_{deal['source']}"
			deal_counts[deal_key] = deal_counts.get(deal_key, 0) + 1

	# Second pass: keep only deals that appear exactly once
	valid_valid_deals = []
	for deal in valid_deals:
			deal_key = f"{deal['title']}_{deal['source']}"
			if deal_counts[deal_key] == 1:  # Only keep if it appears exactly once
					valid_valid_deals.append(deal)

	valid_deals_file = "valid_deals.json"
	with open(valid_deals_file, "w", encoding="utf-8") as f:
		json.dump(valid_valid_deals, f, indent=4, ensure_ascii=False)

	# Close browser only at the very end
	driver.quit()