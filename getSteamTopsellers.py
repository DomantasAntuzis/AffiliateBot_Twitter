import requests
from bs4 import BeautifulSoup
import csv

def fetch_batch(start, count=100, cc="US", lang="en"):
    """Fetch a batch of games from Steam starting at 'start' position"""
    url = "https://store.steampowered.com/search/"
    params = {
        "filter": "globaltopsellers",
        "count": count,
        "start": start,
        "cc": cc,
        "l": lang,
        "category1": 998,      # Only games (not DLCs, software, etc.)
        "hidef2p": 1,          # Hide free-to-play games
        "infinite": 1,
        "force_infinite": 1
    }
    headers = {
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/125.0.0.0 Safari/537.36"),
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "X-Requested-With": "XMLHttpRequest",
        "Referer": "https://store.steampowered.com/search/"
    }

    r = requests.get(url, params=params, headers=headers, timeout=20)
    r.raise_for_status()
    data = r.json()
    
    if "results_html" not in data:
        return []

    html = data["results_html"]
    soup = BeautifulSoup(html, "html.parser")
    results = []

    for row in soup.select(".search_result_row"):
        appid = row.get("data-ds-appid") or row.get("data-ds-packageid") or "?"
        title = row.select_one(".title")
        if not title:
            continue
            
        title_text = title.get_text(strip=True)
        price_el = row.select_one(".discount_final_price")
        price = price_el.get_text(strip=True) if price_el else "N/A"
        discount_el = row.select_one(".discount_pct")
        discount = discount_el.get_text(strip=True) if discount_el else "0%"

        results.append({
            "appid": appid,
            "title": title_text,
            "price": price,
            "discount": discount
        })

    return results

def fetch_top500_topsellers(cc="US", lang="en"):
    """Fetch exactly 500 top sellers from Steam using multiple API calls (Steam limits to 100 per request)"""
    all_results = []
    
    # Steam API limits to 100 results per request, so we need 5 requests for 500 items
    for i in range(5):
        start_pos = i * 100
        print(f"Fetching batch {i+1}/5 (items {start_pos+1}-{start_pos+100})...")
        
        try:
            batch = fetch_batch(start_pos, 100, cc, lang)
            all_results.extend(batch)
            print(f"  ✓ Got {len(batch)} games")
            
            if len(batch) < 100:  # If we get less than 100, we've reached the end
                print(f"  ⚠️  Only {len(batch)} games returned, likely reached end of results")
                break
                
        except Exception as e:
            print(f"  ✗ Error fetching batch {i+1}: {e}")
            break

    print(f"Successfully fetched {len(all_results)} games total using {len(all_results)//100 + (1 if len(all_results)%100 else 0)} API calls")
    return all_results

def scrape_steam_topsellers():
    topsellers = fetch_top500_topsellers()
    
    # Save results to CSV file with only name and price
    with open("csv_data/steamdb_results.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        # Write header
        writer.writerow(["name", "price"])
        # Write data rows
        for game in topsellers:
            writer.writerow([game["title"], game["price"]])
    
    print("Total fetched:", len(topsellers))
    print("Results saved to steam_games.csv")
