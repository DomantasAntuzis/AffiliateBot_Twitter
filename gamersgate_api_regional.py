import requests
import time
import json
import html
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import config


def get_proxy_info(proxies):
    """Extract proxy info for logging"""
    if proxies and 'http' in proxies:
        return proxies['http']
    return "No proxy"


def fetch_gamersgate_page(page=1, platform="pc", timestamp=None, session=None):
    """
    Fetch ONE page of GamersGate offers via API.
    
    Args:
        page: Page number
        platform: Platform filter (default: "pc")
        timestamp: Session timestamp for consistent pagination
        session: requests.Session object (maintains same proxy connection)
    
    Returns:
        dict: API response data or None on failure
    """
    url = "https://www.gamersgate.com/api/offers/"

    # Use provided timestamp or generate new one
    if timestamp is None:
        timestamp = int(time.time() * 1000)

    params = {
        "platform": platform,
        "timestamp": timestamp,
        "need_change_browser_url": "true",
        "activations": 1,
        "dlc": "on",
    }
    
    # Add page parameter for page > 1
    if page > 1:
        params["page"] = page

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:145.0) Gecko/20100101 Firefox/145.0",
        "Accept": "*/*",
        "Referer": f"https://www.gamersgate.com/offers/?platform={platform}",
        "Origin": "https://www.gamersgate.com",
    }

    try:
        # Use session if provided (keeps same proxy connection)
        if session:
            resp = session.get(url, params=params, headers=headers, timeout=30)
        else:
            resp = requests.get(url, params=params, headers=headers, timeout=30)
        
        resp.raise_for_status()
        data = resp.json()
        
        # Log response details
        catalog_size = len(data.get("catalog", []))
        print(f"       [API] Status {resp.status_code}, returned {catalog_size} items")
        
        return data
    except requests.exceptions.Timeout as e:
        print(f"       [ERROR] Timeout: {e}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"       [ERROR] Request failed: {e}")
        return None
    except Exception as e:
        print(f"       [ERROR] Unexpected error: {e}")
        return None


def parse_item(item):
    """
    Extract relevant fields from API item.
    
    Args:
        item: Single item from API catalog
    
    Returns:
        dict: Parsed item data
    """
    # Clean baseprice (remove HTML entities and currency symbols)
    baseprice = item.get("baseprice", "")
    if baseprice:
        baseprice = html.unescape(baseprice)
        baseprice = baseprice.replace("&nbsp;", " ").replace("€", "").replace("$", "").replace("£", "").replace("¥", "").strip()
    
    raw_price = item.get("raw_price", "").strip()
    if raw_price:
        raw_price = raw_price.replace("€", "").replace("$", "").replace("£", "").replace("¥", "").strip()

    return {
        "name": item.get("name", ""),
        "discount_percent": round(item.get("discount_percent", 0)),
        "sale_price": raw_price,
        "list_price": baseprice,
        "is_available": item.get("is_available", False),
    }


def fetch_all_gamersgate_offers():
    """
    Fetch all GamersGate offers using API with consistent session.
    Uses requests.Session() to maintain same proxy connection/IP.
    
    Returns:
        list: All parsed offers
    """
    # Setup proxy
    proxies = {}
    if hasattr(config, 'ROTATING_PROXY') and config.ROTATING_PROXY:
        proxy_url = str(config.ROTATING_PROXY).split(',')[0].split(';')[0].strip()
        proxies = {
            'http': proxy_url,
            'https': proxy_url
        }
        print(f"[PROXY CONFIG] Using: {proxy_url}")
    else:
        print("[WARNING] No proxy configured - will use local region")
    
    # Create a Session object to maintain same connection (and same proxy IP)
    session = requests.Session()
    if proxies:
        session.proxies.update(proxies)
    
    # Verify proxy IP before starting
    if proxies:
        try:
            print("[VERIFYING] Checking actual proxy IP...")
            ip_check = session.get("https://api.ipify.org?format=json", timeout=10)
            actual_ip = ip_check.json().get('ip', 'Unknown')
            print(f"[CONFIRMED] Proxy IP: {actual_ip}")
            print(f"[LOCKED] Session will maintain this same IP for all requests\n")
        except Exception as e:
            print(f"[WARNING] Could not verify proxy IP: {e}\n")
    else:
        print()
    
    all_offers = []
    platform = "pc"
    page = 1
    previous_page_names = None
    
    # IMPORTANT: Use shared timestamp for consistent pagination within session
    session_timestamp = int(time.time() * 1000)
    print(f"[SESSION] Timestamp: {session_timestamp}")
    print("[INFO] Using shared timestamp for consistent catalog snapshot\n")
    
    consecutive_failures = 0
    max_consecutive_failures = 3
    
    print("Fetching GamersGate pages via API...")
    
    while True:
        print(f"\n[Page {page}] Fetching...")
        
        # Use shared timestamp and session for all pages
        data = fetch_gamersgate_page(
            page=page, 
            platform=platform, 
            timestamp=session_timestamp,
            session=session
        )
        
        if not data:
            consecutive_failures += 1
            print(f"       [FAIL] No data returned (failure {consecutive_failures}/{max_consecutive_failures})")
            if consecutive_failures >= max_consecutive_failures:
                print(f"       [STOP] Stopping after {max_consecutive_failures} consecutive failures")
                break
            # Try next page anyway
            page += 1
            time.sleep(1)
            continue

        catalog = data.get("catalog", [])
        if not catalog:
            print(f"       [STOP] Empty catalog returned")
            break

        parsed_items = [parse_item(i) for i in catalog]
        current_names = [item.get("name") for item in parsed_items]

        print(f"       [PARSED] {len(parsed_items)} items (total: {len(all_offers) + len(parsed_items)})")

        # Check if this page is duplicate of previous page
        if previous_page_names is not None:
            # Compare first 5 items for robustness (API can be slightly inconsistent)
            current_sample = current_names[:5] if len(current_names) >= 5 else current_names
            previous_sample = previous_page_names[:5] if len(previous_page_names) >= 5 else previous_page_names
            
            if current_sample == previous_sample:
                print(f"       [DUPLICATE] Page {page} matches page {page-1}, stopping")
                break

        all_offers.extend(parsed_items)
        previous_page_names = current_names
        consecutive_failures = 0  # Reset on success

        page += 1
        time.sleep(2)  # Polite rate limit
        
        # Safety limit
        if page > 50:
            print(f"       [STOP] Reached safety limit of 50 pages")
            break
    
    # Close session
    session.close()
    
    return all_offers


if __name__ == "__main__":
    print("="*70)
    print("GamersGate API Fetcher - Regional Catalog Testing")
    print("="*70)
    print()
    
    try:
        all_offers = fetch_all_gamersgate_offers()
        
        print(f"\n{'='*70}")
        print(f"[SUCCESS] Total collected: {len(all_offers)} offers")
        print(f"{'='*70}\n")
        
        # Save to JSON
        output_file = "GamersGate_api_offers.json"
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(all_offers, f, indent=4, ensure_ascii=False)
        
        print(f"[SAVED] {output_file}")
        
        # Show sample of collected data
        if all_offers:
            print(f"\n[SAMPLE] First 3 offers:")
            for i, offer in enumerate(all_offers[:3], 1):
                print(f"  {i}. {offer['name']} - {offer['discount_percent']}% off (${offer['sale_price']} from ${offer['list_price']})")
        
    except KeyboardInterrupt:
        print("\n[INTERRUPTED] Fetch interrupted by user")
    except Exception as e:
        print(f"\n[ERROR] Fetch failed: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n[DONE]")

