"""
Twitter posting service
Handles tweet creation and posting to Twitter
"""
import os
import tweepy
import requests
import sys

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import config
from database.db_connect import get_connection, close_connection, execute_query
from utils.logger import logger
from utils.helpers import add_posted_game

def post_deal_to_twitter(deal):
    """
    Post a deal to Twitter
    
    Args:
        deal: Deal dictionary with keys: title, source, link, image_link, salePrice, discount
    
    Returns:
        bool: True if successful, False otherwise
    """
    deal_title = deal["title"]
    deal_source = deal["source"]
    deal_link = deal["link"]
    deal_image_link = deal["image_link"]
    deal_sale_price = deal["salePrice"]
    deal_discount = deal["discount"]
    
    logger.info(f"Posting tweet for: {deal_title} ({deal_source})")
    
    # Download and save image
    image_path = _download_game_image(deal_image_link)
    if not image_path:
        logger.error("Failed to download game image")
        return False
    
    # Authenticate to Twitter
    auth_result = _authenticate_twitter()
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
    formatted_source = _format_source_name(deal_source)
    
    # Create tweet text
    tweet = f"[{formatted_source}] {deal_title} - {deal_discount}% OFF!\nNow {deal_sale_price}\n{deal_link}\n\n#PCGaming #GameDeals #{formatted_source}"
    
    # Post tweet
    try:
        response = client.create_tweet(text=tweet, media_ids=[media.media_id])
        logger.info(f"Successfully posted tweet for {deal_title}")
        
        # Add to posted games list
        add_posted_game(deal_title, config.POSTED_GAMES_FILE, config.POSTED_GAMES_LIMIT)
        
        return True
        
    except Exception as e:
        logger.error(f"Error posting tweet: {e}")
        return False

def _download_game_image(image_url):
    """
    Download game image from URL
    
    Args:
        image_url: URL of the image
    
    Returns:
        str: Path to saved image or None if failed
    """
    os.makedirs(config.IMAGES_DIR, exist_ok=True)
    image_path = os.path.join(config.IMAGES_DIR, "game_image.jpg")
    
    try:
        img_data = requests.get(image_url).content
        
        with open(image_path, 'wb') as handler:
            handler.write(img_data)
        
        logger.debug(f"Image downloaded to {image_path}")
        return image_path
        
    except requests.RequestException as e:
        logger.error(f"Error downloading image: {e}")
        return None
    except IOError as e:
        logger.error(f"Error saving image: {e}")
        return None

def _authenticate_twitter():
    """
    Authenticate with Twitter API
    
    Returns:
        tuple: (API object, Client object) or None if failed
    """
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

def _format_source_name(source):
    """
    Format source name for tweet display
    
    Args:
        source: Original source name
    
    Returns:
        str: Formatted source name
    """
    if source == "GOG.COM INT":
        return "GOG"
    elif source == "GamersGate.com":
        return "GamersGate"
    else:
        return source

