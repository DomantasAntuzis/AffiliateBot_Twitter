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
# find_deals();

def post_tweet(deal):
			
	deal_title = deal["title"]
	deal_source = deal["source"]
	deal_link = deal["link"]
	deal_image_link = deal["image_link"]
	deal_salePrice = deal["salePrice"]
	deal_discount = deal["discount"]

	os.makedirs("game_images", exist_ok=True)
	
	try:
		img_data = requests.get(deal_image_link).content
		
		with open(os.path.join("game_images", "game_image.jpg"), 'wb') as handler:
			handler.write(img_data)
	except requests.RequestException as e:
		print(f"Error downloading image: {e}")
		return None
	except IOError as e:
		print(f"Error saving image: {e}")
		return None

	# Authentication credentials
	bearer_token = os.getenv("TWITTER_BEARER_TOKEN")
	api_key = os.getenv("TWITTER_API_KEY")
	api_key_secret = os.getenv("TWITTER_API_KEY_SECRET")   
	access_token = os.getenv("TWITTER_ACCESS_TOKEN")
	access_token_secret = os.getenv("TWITTER_ACCESS_TOKEN_SECRET")

	# Validate all required environment variables are set
	required_vars = [bearer_token, api_key, api_key_secret, access_token, access_token_secret]
	if not all(required_vars):
		print("Error: Missing required Twitter API credentials in environment variables")
		return None

	auth = tweepy.OAuth1UserHandler(api_key, api_key_secret, access_token, access_token_secret)
	api = tweepy.API(auth)

	# Authenticate to Twitter using OAuth 2.0 Bearer Token
	client = tweepy.Client(bearer_token=bearer_token, 
												consumer_key=api_key, 
												consumer_secret=api_key_secret,
												access_token=access_token, 
												access_token_secret=access_token_secret)

	# Upload the image using the API object
	try:
		media = api.media_upload(filename=os.path.join("game_images", "game_image.jpg"))
	except Exception as e:
		print(f"Error uploading media: {e}")
		return None

	if deal_source == "GOG.COM INT":
		deal_source = "GOG"

	# Post a tweet
	tweet = f"[{deal_source}] {deal_title} - {deal_discount} OFF!\nNow ${deal_salePrice}.\n{deal_link}\n\n#PCGaming #GameDeals #{deal_source}"
	try:
		response = client.create_tweet(text=tweet, media_ids=[media.media_id])
	except Exception as e:
		print(f"Error posting tweet: {e}")
		return None

	with open("posted_games.txt", "a") as f:
		f.write(f"{deal_title}\n")

	print(f"posted {deal_title} {deal_source} time:", response)
	# time.sleep(60*60*24)

# test_json = {
#         "source": "YUPLAY",
#         "title": "Age of Empires IV: Anniversary Edition",
#         "link": "https://www.kqzyfj.com/click-101471996-15862927?url=https%3A%2F%2Fwww.yuplay.com%2Fproduct%2Fage-of-empires-iv-anniversary-edition%2F%3Fpartner%3Dcj%3Fpartner%3D%7B%7B+invite_code+%7D%7D",
#         "image_link": "https://www.yuplay.com/media/products/age-of-empires-iv-anniversary-edition/616/5d851fa0b66cab235cd73ba0ce852afa5d943c49.jpg",
#         "discount": "67",
#         "salePrice": 15.59
#     }

# post_tweet(test_json)