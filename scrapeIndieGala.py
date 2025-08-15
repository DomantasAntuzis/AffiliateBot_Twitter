from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import json
import csv

def scrape_indiegala():
	url = "https://www.indiegala.com/store/games/on-sale"
	rotating_proxy = "http://p.webshare.io:9999"

	chrome_options = Options()
	prefs = {'profile.default_content_setting_values': {'cookies': 2, 'images': 2, 
															'plugins': 2, 'popups': 2, 'geolocation': 2, 
															'notifications': 2, 'auto_select_certificate': 2, 'fullscreen': 2, 
															'mouselock': 2, 'mixed_script': 2, 'media_stream': 2, 
															'media_stream_mic': 2, 'media_stream_camera': 2, 'protocol_handlers': 2, 
															'ppapi_broker': 2, 'automatic_downloads': 2, 'midi_sysex': 2, 
															'push_messaging': 2, 'ssl_cert_decisions': 2, 'metro_switch_to_desktop': 2, 
															'protected_media_identifier': 2, 'app_banner': 2, 'site_engagement': 2, 
															'durable_storage': 2}}
	
	chrome_options.add_experimental_option('prefs', prefs)
	chrome_options.add_argument("--headless")
	chrome_options.add_argument("--disable-gpu")
	chrome_options.add_argument("--window-size=1920,1080")
	chrome_options.add_argument("--no-sandbox")
	chrome_options.add_argument("--disable-blink-features=AutomationControlled")
	chrome_options.add_argument(f"--proxy-server={rotating_proxy}")

	driver = webdriver.Chrome(options=chrome_options)
	
	try:
		driver.get(url)
		wait = WebDriverWait(driver, 5)
		wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".relative.main-list-results-item")))

		gameDeals = []
		nextNr = 2
		while True:
			gameCards = driver.find_elements(By.CSS_SELECTOR, ".relative.main-list-results-item")
			# print(len(gameCards))
			for gameCard in gameCards:
				try:
					game_title = gameCard.find_element(By.CSS_SELECTOR, "h3.bg-gradient-red").text
					game_discount = gameCard.find_element(By.CSS_SELECTOR, "div.main-list-results-item-discount").text.replace("%", "").replace("-", "")
					game_price = gameCard.find_element(By.CSS_SELECTOR, "div.main-list-results-item-price-new").text
					game_link = gameCard.find_element(By.CSS_SELECTOR, "figure.relative a").get_attribute("href")
					game_affiliate_link = game_link + '?ref=mzvkywq'
					game_image = gameCard.find_element(By.CSS_SELECTOR, "figure.relative img.async-img-load.display-none").get_attribute("src")
					games = {
						"source": "IndieGala",
						"title": game_title,
						"discount": int(game_discount),
						"salePrice": game_price.replace(" ", ""),
						"link": game_affiliate_link,
						"image_link": game_image
					}
					gameDeals.append(games)
				except Exception as e:
					print(e)

			try:
				next_button = driver.find_element(By.CSS_SELECTOR, f"a[onclick*='/{nextNr}']")
				
				if next_button.is_displayed():
					print(f"Clicking page {nextNr}")
					driver.execute_script("arguments[0].click();", next_button)  # Use JS click
					nextNr += 1
					wait.until(EC.staleness_of(gameCards[0]))  # Wait for old content to disappear
					wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".relative.main-list-results-item")))  # Wait for new content
				else:
					print("No more pages or next button not visible")
					break
			except Exception as e:
				print(f"No next button found for page {nextNr} - finished scraping: {e}")
				break
	finally:
		driver.quit()

	#delete games not in top 500
	with open("csv_data/steamdb_results.csv", encoding="utf-8") as f:
		steamdb_games = list(csv.reader(f))
	steamdb_titles = [row[0] for row in steamdb_games]
	gameDeals = [game for game in gameDeals if game["title"] in steamdb_titles]

	with open("posted_games.txt", encoding="utf-8") as f:
		posted_games_list = f.read().splitlines()
	
	# remove posted games
	for posted_game in posted_games_list:
		game_title = posted_game
		for game in gameDeals:
			if game["title"] == game_title:
				gameDeals.remove(game)

#compare prices with steamdb games
	with open("csv_data/steamdb_results.csv", encoding="utf-8") as f:
		steamdb_games = csv.reader(f)
		for game in gameDeals:
				for steamdb_game in steamdb_games:
						if game["title"] == steamdb_game[0]:
								steamGamePrice = steamdb_game[1].replace("$", "")
								gameSalePrice = game["salePrice"].replace("$", "").replace(" ", "")
								if float(gameSalePrice) >= float(steamGamePrice):
										gameDeals.remove(game)

	#read and append valid deals
	valid_deals = "valid_deals.json"
	with open(valid_deals, encoding="utf-8") as f:
		valid_deals_list = json.load(f)
	valid_deals_list.append(gameDeals)
	with open(valid_deals, "w", encoding="utf-8") as f:
		json.dump(valid_deals_list, f, indent=4, ensure_ascii=False)

if __name__ == "__main__":
	scrape_indiegala()