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

		with open("posted_games.txt", encoding="utf-8") as f:
			posted_games_list = f.read().splitlines()

		try:

			#handle picking deal that hasnt been posted yet
			while True:
				rng = random.randint(0, len(deals) - 1)
				print(rng)
				deal = deals[rng][0]

				if deal["title"] in posted_games_list or deal["discount"] <= 10:
					deals[rng].remove(deal)
					#remove empty list from deals
					if deals[rng] == []:
						deals.remove(deals[rng])
					continue
				else:
					break

			print(deal)

			post_tweet(deal)



		except Exception as e:
			print(e)
			continue

		post_count += 1
		if post_count == 4:
			break
		
		time.sleep(5*60*60)

#set for next day to 17:00 ; 4 posts ; 3.5 hours between them
schedule.every().day.at("10:30").do(daily)

print("🚀 Bot started! Waiting for scheduled tasks...")
print("📅 Daily job scheduled for 00:00 (midnight)")
print("⏰ Next run:", schedule.next_run())

while True:
	schedule.run_pending()
	time.sleep(60)