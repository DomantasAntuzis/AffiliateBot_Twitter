"""
Twitter posting service
Handles tweet creation and posting to Twitter
"""
import io
import os
import tweepy
import requests
from PIL import Image


import config
from utils.logger import logger
from database.queries.items import get_offer_id
from database.queries.twitter_posts import insert_twitter_post
from utils.helpers import normalize_distributor_name

def post_deal_to_twitter(deal):
  deal_title = deal["title"]
  deal_source = deal["source"]
  deal_link = deal["link"]
  deal_image_link = deal["image_link"]
  deal_sale_price = deal["salePrice"]
  deal_discount = deal.get("discount", 0)
  if isinstance(deal_discount, float):
    deal_discount = int(round(deal_discount))
  elif not isinstance(deal_discount, int):
    try:
      deal_discount = int(round(float(deal_discount)))
    except (ValueError, TypeError):
      deal_discount = 0

  logger.info(f"Posting tweet for: {deal_title} ({deal_source})")

  # Download and save image
  image_path = download_game_image(deal_image_link)
  if not image_path:
    logger.error("Failed to download game image")
    return False

  # Authenticate to Twitter
  auth_result = authenticate_twitter()
  if not auth_result:
    logger.error("Failed to authenticate with Twitter")
    return False

  api, client = auth_result

  # Upload image
  try:
    media = api.media_upload(filename=image_path)
  except Exception as e:
    logger.error(f"Error uploading media: {e}")
    return False

  # Format source name
  formatted_source = format_source_name(deal_source)

  # Create tweet text
  price_str = f"${deal_sale_price:.2f}" if isinstance(deal_sale_price, (int, float)) else str(deal_sale_price)
  tweet = f"[{formatted_source}] {deal_title} - {deal_discount}% OFF!\nNow {price_str}\n{deal_link}\n\n#PCGaming #GameDeals #{formatted_source}"

  # Post tweet
  try:
    response = client.create_tweet(text=tweet, media_ids=[media.media_id])

    # Log response details
    if response:
      try:
        normalized_distributor = normalize_distributor_name(deal_source)
        offer_id = get_offer_id(
          item_title=deal_title,
          affiliate_url=deal_link,
          image_url=deal_image_link,
          distributor_name=normalized_distributor,
        )
        if offer_id:
          insert_twitter_post(offer_id)
          logger.info(f"Recorded posted game in database. Offer ID: {offer_id}")
        else:
          logger.warning(f"Offer entry not found for posted deal: {deal_title} (distributor: {normalized_distributor})")
      except Exception as e:
        logger.error(f"Error recording posted game in database: {e}")

      logger.info(f"Tweet posted successfully for {deal_title}")
      logger.info(f"Tweet ID: {response.data.get('id') if hasattr(response, 'data') else 'N/A'}")
      logger.debug(f"Full response: {response}")
    else:
      logger.warning(f"Tweet posted but received empty response for {deal_title}")

    return True

  except tweepy.TweepyException as e:
    logger.error(f"Twitter API error posting tweet: {e}")
    logger.error(f"Error details: {e.response if hasattr(e, 'response') else 'No response data'}")
    if hasattr(e, 'api_code'):
      logger.error(f"API error code: {e.api_code}")
    if hasattr(e, 'api_messages'):
      logger.error(f"API messages: {e.api_messages}")
    return False
  except Exception as e:
    logger.error(f"Unexpected error posting tweet: {e}")
    logger.error(f"Error type: {type(e).__name__}")
    return False

def download_game_image(image_url):
  os.makedirs(config.IMAGES_DIR, exist_ok=True)
  image_path = os.path.join(config.IMAGES_DIR, "game_image.jpg")

  try:
    resp = requests.get(image_url, timeout=15)
    resp.raise_for_status()
    img_data = resp.content
    content_type = (resp.headers.get("Content-Type") or "").lower()

    if "image/" not in content_type:
      logger.warning(f"URL returned non-image Content-Type: {content_type}")

    img = Image.open(io.BytesIO(img_data))
    if img.mode in ("RGBA", "P"):
      img = img.convert("RGB")
    img.save(image_path, "JPEG", quality=90)
    logger.debug(f"Image converted and saved to {image_path}")
    return image_path

  except requests.RequestException as e:
    logger.error(f"Error downloading image: {e}")
    return None
  except IOError as e:
    logger.error(f"Error saving image: {e}")
    return None
  except Exception as e:
    logger.error(f"Error processing image (may be invalid format): {e}")
    return None

def authenticate_twitter():
  bearer_token = config.TWITTER_BEARER_TOKEN
  api_key = config.TWITTER_API_KEY
  api_key_secret = config.TWITTER_API_KEY_SECRET
  access_token = config.TWITTER_ACCESS_TOKEN
  access_token_secret = config.TWITTER_ACCESS_TOKEN_SECRET

  # Validate all required environment variables are set
  required_vars = [bearer_token, api_key, api_key_secret, access_token, access_token_secret]
  if not all(required_vars):
    logger.error("Missing required Twitter API credentials in environment variables")
    return None

  try:
    # OAuth 1.0a authentication for media upload
    auth = tweepy.OAuth1UserHandler(api_key, api_key_secret, access_token, access_token_secret)
    api = tweepy.API(auth)

    # OAuth 2.0 Bearer Token for tweet posting
    client = tweepy.Client(
      bearer_token=bearer_token,
      consumer_key=api_key,
      consumer_secret=api_key_secret,
      access_token=access_token,
      access_token_secret=access_token_secret
    )

    return api, client

  except Exception as e:
    logger.error(f"Error authenticating with Twitter: {e}")
    return None

def format_source_name(source):
  if source == "GOG.COM INT":
    return "GOG"
  elif source == "GamersGate.com":
    return "GamersGate"
  else:
    return source