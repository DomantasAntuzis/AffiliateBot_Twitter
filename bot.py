from scrapeSteamdb import scrape_steamdb
from getProducts import get_products
from findDeals import find_deals

import os
import tweepy
import csv
import json
import requests
import time

# get_products();
# scrape_steamdb();
find_deals();

# def post_tweet():

# 	#loop though deals.json
# 	with open("deals.json", "r") as f:
# 		deals = json.load(f)

# 	with open("posted_games.txt", "r") as f:
# 		posted_games = f.read().splitlines()
# 		temp_posted_games = posted_games

# 	#F you YUPLAY with your duplicates
# 	deal_counts = {}
# 	for deal in deals:
# 			deal_key = f"{deal['tittle']}_{deal['source']}"
# 			deal_counts[deal_key] = deal_counts.get(deal_key, 0) + 1

# 	duplicates_to_skip = set()
# 	for deal_key, count in deal_counts.items():
# 			if count > 1:
# 					title = deal_key.split('_')[0]
# 					if title not in posted_games:
# 							duplicates_to_skip.add(title)
# 							with open("posted_games.txt", "a") as f:
# 									f.write(f"{title}\n")

# 	# Update posted_games list to include the new duplicates
# 	posted_games.extend(duplicates_to_skip)

# 	for deal in deals:

# 		#deal that was not posted before
# 		if deal["tittle"] in temp_posted_games:
# 			continue
# 		else:
# 			deal_tittle = deal["tittle"]
# 			deal_source = deal["source"]

# 			#deal data
# 			deal_price = deal["price"]
# 			deal_link = deal["link"]
# 			deal_image_link = deal["image_link"]
# 			deal_salePrice = deal["salePrice"]
# 			deal_discount = deal["discount"]

# 			os.makedirs("game_images", exist_ok=True)
# 			img_data = requests.get(deal_image_link).content
			
# 			with open(os.path.join("game_images", "game_image.jpg"), 'wb') as handler:
# 				handler.write(img_data)

# 			# Authentication credentials
# 			# bearer_token = os.getenv("TWITTER_BEARER_TOKEN")
# 			# api_key = os.getenv("TWITTER_API_KEY")
# 			# api_key_secret = os.getenv("TWITTER_API_KEY_SECRET")   
# 			# access_token = os.getenv("TWITTER_ACCESS_TOKEN")
# 			# access_token_secret = os.getenv("TWITTER_ACCESS_TOKEN_SECRET")

# 			# auth = tweepy.OAuth1UserHandler(api_key, api_key_secret, access_token, access_token_secret)
# 			# api = tweepy.API(auth)


# 			# # Authenticate to Twitter using OAuth 2.0 Bearer Token
# 			# client = tweepy.Client(bearer_token=bearer_token, 
# 			# 											consumer_key=api_key, 
# 			# 											consumer_key_secret=api_key_secret, 
# 			# 											access_token=access_token, 
# 			# 											access_token_secret=access_token_secret)
	
# 			# # Upload the image using the API object
# 			# media = api.media_upload(filename="game_images", file="game_image.jpg")

# 			# if deal_source == "GOG.COM INT":
# 			# 	deal_source = "GOG"

# 			# # # Post a tweet
# 			# tweet = f"[{deal_source}] {deal_tittle} - {deal_discount} OFF!\nNow just ${deal_salePrice}.\n{deal_link}\n\n#PCGaming #GameDeals #{deal_source}"
# 			# response = client.create_tweet(text=tweet, media_ids=[media.media_id])


# 			with open("posted_games.txt", "a") as f:
# 				f.write(f"{deal_tittle}\n")

# 			temp_posted_games.append(deal_tittle)

# 			# print(tweet)

# 			print(f"posted {deal_tittle} {deal_source} time")
# 			time.sleep(60*60*24)

# 		# print("Tweet posted successfully!", response)

# # post_tweet()