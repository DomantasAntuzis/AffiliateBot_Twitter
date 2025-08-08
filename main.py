from getProducts import get_products
from findDeals import find_deals
from scrapeSteamdb import scrape_steamdb
from validateDeals import validate_deals_export
from twitterBot import post_tweet
from scrapeIndieGala import scrape_indiegala

import schedule
import time
import random
import json

def daily():

	get_products();
	scrape_steamdb();
	deals = find_deals();
	validate_deals_export(deals);
	scrape_indiegala();

	with open("valid_deals.json", "r") as f:
		deals = json.load(f)

	for group in deals:
		random.shuffle(group)

	with open("shuffled_deals.json", "w") as f:
		json.dump(deals, f, indent=4, ensure_ascii=False)

	post_count = 0
	while True:

		try:
			rng = random.randint(0, len(deals) - 1)
			print(rng)
			#might not exist i > 1 but
			deal = deals[rng][0]
			deals[rng].remove(deal)

			if deals[rng] == []:
				deals.remove(deals[rng])

			print(deal)

			post_tweet(deal)
		except Exception as e:
			print(e)
			continue

		post_count += 1
		if post_count == 3:
			break
		
		time.sleep(6*60*60)

schedule.every().day.at("12:30").do(daily)

print("🚀 Bot started! Waiting for scheduled tasks...")
print("📅 Daily job scheduled for 00:00 (midnight)")
print("⏰ Next run:", schedule.next_run())

while True:
	schedule.run_pending()
	time.sleep(60)