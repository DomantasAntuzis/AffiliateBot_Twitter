import time
import json
import sys
import os
import re
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import config


def setup_driver():
    """Setup Chrome driver with proxy"""
    chrome_options = Options()
    
    # Minimal prefs - keep CSS/JS enabled for proper rendering
    prefs = {
        'profile.default_content_setting_values': {
            'cookies': 1,
            'images': 2,   # Disable images only
            'popups': 2,
            'geolocation': 2,
            'notifications': 2,
        }
    }
    
    chrome_options.add_experimental_option('prefs', prefs)
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    
    # Add proxy
    if hasattr(config, 'ROTATING_PROXY') and config.ROTATING_PROXY:
        chrome_options.add_argument(f"--proxy-server={config.ROTATING_PROXY}")
    
    driver = webdriver.Chrome(options=chrome_options)
    driver.set_page_load_timeout(30)
    driver.implicitly_wait(5)
    
    return driver


def parse_game_card(card):
    """
    Parse a single game card to extract title, prices, and discount
    
    Args:
        card: WebElement representing a game card
    
    Returns:
        dict: Parsed game data or None if parsing fails
    """
    try:
        # Try different selectors for title
        title = None
        try:
            title = card.find_element(By.CSS_SELECTOR, ".catalog-item--title a").text.strip()
        except:
            try:
                title = card.find_element(By.CSS_SELECTOR, ".catalog-item--title").text.strip()
            except:
                return None  # No title, skip this card
        
        if not title:
            return None
        
        # Sale price - from .catalog-item--price span
        sale_price_elem = card.find_element(By.CSS_SELECTOR, ".catalog-item--price span")
        sale_price = sale_price_elem.text.strip()
        
        # List price - from .catalog-item--full-price
        list_price_elem = card.find_element(By.CSS_SELECTOR, ".catalog-item--full-price")
        list_price = list_price_elem.text.strip()
        
        # Discount % - from product label
        discount_elem = card.find_element(By.CSS_SELECTOR, ".product--label-discount")
        discount_text = discount_elem.text.strip()
        # Extract number from "-20%" format
        discount_match = re.search(r'(\d+)', discount_text)
        discount_percent = int(discount_match.group(1)) if discount_match else 0
        
        # Clean prices (remove currency symbols)
        sale_price_clean = sale_price.replace("$", "").replace("€", "").replace("£", "").replace("¥", "").strip()
        list_price_clean = list_price.replace("$", "").replace("€", "").replace("£", "").replace("¥", "").strip()
        
        return {
            "name": title,
            "sale_price": sale_price_clean,
            "list_price": list_price_clean,
            "discount_percent": discount_percent,
        }
    except Exception as e:
        # Silently skip unparseable cards
        return None


def get_total_pages(driver):
    """
    Extract total number of pages from pagination element
    
    Args:
        driver: Selenium WebDriver
    
    Returns:
        int: Total number of pages
    """
    try:
        # Find all pagination links with data-page attribute
        page_links = driver.find_elements(By.CSS_SELECTOR, "a[data-page]")
        
        if page_links:
            # Get the highest page number
            max_page = 1
            for link in page_links:
                page_num = link.get_attribute("data-page")
                if page_num and page_num.isdigit():
                    max_page = max(max_page, int(page_num))
            print(f"[DEBUG] Found page numbers: {[link.get_attribute('data-page') for link in page_links]}")
            return max_page
        
        print("[DEBUG] No pagination links found")
        return 1
    except Exception as e:
        print(f"[WARN] Could not determine total pages: {e}")
        return 1


def scrape_gamersgate_page(driver, page=1):
    """
    Scrape one page of GamersGate offers
    
    Args:
        driver: Selenium WebDriver
        page: Page number to scrape
    
    Returns:
        list: List of parsed game dictionaries
    """
    url = f"https://www.gamersgate.com/offers/?platform=pc&activations=1&dlc=on"
    if page > 1:
        url += f"&page={page}"
    
    try:
        driver.get(url)
        
        # Wait for catalog items to load
        wait = WebDriverWait(driver, 10)
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".catalog-item-container")))
        
        # Give extra time for all items to render
        time.sleep(2)
        
        # Find all game cards
        # NEW: Only select cards inside .content-container to exclude popups
        cards = driver.find_elements(By.CSS_SELECTOR, ".content-container .catalog-item-container")

        print(f"       Found {len(cards)} game cards on page")
        
        # Parse each card
        games = []
        for card in cards:
            game_data = parse_game_card(card)
            if game_data:
                games.append(game_data)
        
        return games
        
    except Exception as e:
        print(f"[ERROR] Failed to scrape page {page}: {e}")
        return []


if __name__ == "__main__":
    # Show proxy status
    if hasattr(config, 'ROTATING_PROXY') and config.ROTATING_PROXY:
        print(f"[PROXY] Using: {config.ROTATING_PROXY}")
    else:
        print("[WARNING] No proxy configured")
    
    print("[SCRAPER] Web scraping mode (Selenium)")
    print()
    
    # Setup driver
    print("[SETUP] Initializing Chrome driver...")
    driver = setup_driver()
    
    try:
        all_offers = []
        
        # Step 1: Load first page to get total pages
        print("\n[Page 1] Loading first page...")
        first_page_games = scrape_gamersgate_page(driver, page=1)
        
        if not first_page_games:
            print("[ERROR] Failed to load first page")
            driver.quit()
            exit(1)
        
        print(f"   -> Collected {len(first_page_games)} games")
        all_offers.extend(first_page_games)
        
        # Get total pages from pagination
        total_pages = get_total_pages(driver)
        print(f"\n[PAGINATION] Detected {total_pages} total pages")
        
        # Step 2: Scrape remaining pages
        if total_pages > 1:
            print(f"[SCRAPING] Collecting pages 2-{total_pages}...")
            
            for page in range(2, total_pages + 1):
                print(f"\n[Page {page}] Scraping...")
                
                games = scrape_gamersgate_page(driver, page=page)
                
                if games:
                    print(f"   -> Collected {len(games)} games (total: {len(all_offers) + len(games)})")
                    all_offers.extend(games)
                else:
                    print(f"   -> No games found on page {page}")
                
                # Polite delay between pages
                time.sleep(2)
        
        print(f"\n[SUCCESS] Total collected: {len(all_offers)} games from {total_pages} pages")
        
        # Save to JSON
        with open("GamersGate_all_offers.json", "w", encoding="utf-8") as f:
            json.dump(all_offers, f, indent=4, ensure_ascii=False)
        
        print("[SAVED] GamersGate_all_offers.json")
        
    except KeyboardInterrupt:
        print("\n[INTERRUPTED] Scraping interrupted by user")
    except Exception as e:
        print(f"\n[ERROR] Scraping failed: {e}")
        import traceback
        traceback.print_exc()
    finally:
        print("\n[CLEANUP] Closing browser...")
        driver.quit()
        print("[DONE]")
