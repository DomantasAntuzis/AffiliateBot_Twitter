from getProducts import get_products
from findDeals import find_deals
from scrapeSteamdb import scrape_steamdb
from validateDeals import validate_deals_export
from twitterBot import post_tweet
from scrapeIndieGala import scrape_indiegala
import datetime

import schedule
import time
import random
import json

def daily():
	
	start_time = time.time()
	get_products();
	scrape_steamdb();
	deals = find_deals();
	validate_deals_export(deals);
	scrape_indiegala();
	end_time = time.time()
	
	elapsed_time = end_time - start_time
	print(f"Data collection and validation completed in {elapsed_time:.2f} seconds")

	with open("valid_deals.json", "r") as f:
		deals = json.load(f)

	for group in deals:
		random.shuffle(group)

	with open("shuffled_deals.json", "w") as f:
		json.dump(deals, f, indent=4, ensure_ascii=False)

	post_count = 0
	while True:

		with open("posted_games.txt", encoding="utf-8") as f:
			posted_games_list = f.read().splitlines()

		try:

			#handle picking deal that hasnt been posted yet
			while True:
				if not deals:
					raise RuntimeError("No deals left to post")

				rng = random.randint(0, len(deals) - 1)
				print(rng)

				if not deals[rng]:
					del deals[rng]
					continue

				deal = deals[rng][0]

				if deal["title"] in posted_games_list or deal["discount"] <= 10:
					deals[rng].pop(0)
					#remove empty list from deals
					if not deals[rng]:
						del deals[rng]
					continue
				else:
					break

			post_tweet(deal)

			deals[rng].pop(0)
			if not deals[rng]:
				del deals[rng]

		except Exception as e:
			print(e)
			continue

		post_count += 1
		if post_count == 6:
			break
		
		time.sleep(4*60*60)

#set for next day to 17:00 ; 4 posts ; 3.5 hours between them
now = datetime.datetime.now() + datetime.timedelta(minutes=2)
run_time = now.strftime("%H:%M")
schedule.every().day.at(run_time).do(daily)

print("🚀 Bot started! Waiting for scheduled tasks...")
print("📅 Daily job scheduled for 00:00 (midnight)")
print("⏰ Next run:", schedule.next_run())

while True:
	schedule.run_pending()
	time.sleep(60)