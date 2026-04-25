"""
Steam API integration service
Handles fetching Steam top sellers data
"""
import requests
from bs4 import BeautifulSoup
import re
from sqlmodel import select

import config
from utils.logger import logger
from utils.helpers import normalize_match_title
from database.queries.steam import insert_topsellers
from database.db_session import get_session
from database.models import Item, TopSeller

def fetch_batch(start, count=100, cc="US", lang="en"):
    session = requests.Session()
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
    
    try:
        r = session.get(url, params=params, headers=headers, timeout=20)
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
        
    except Exception as e:
        logger.error(f"Error fetching Steam batch: {e}")
        return []

def fetch_top500_topsellers(cc="US", lang="en"):
    all_results = []
    
    # Steam API limits to 100 results per request, so we need 5 requests for 500 items
    for i in range(5):
        start_pos = i * 100
        logger.info(f"Fetching batch {i+1}/5 (items {start_pos+1}-{start_pos+100})...")
        
        try:
            batch = fetch_batch(start_pos, 100, cc, lang)
            all_results.extend(batch)
            
            if len(batch) < 100:
                logger.warning(f"Only {len(batch)} games returned, likely reached end of results")
                break
                
        except Exception as e:
            logger.error(f"Error fetching batch {i+1}: {e}")
            break
    
    logger.info(f"Total fetched: {len(all_results)} games")
    return all_results

def parse_price(price_str):
    if not price_str or price_str in ["Free", "N/A", "?", ""]:
        return 0.00
    
    try:
        # Remove currency symbols, commas, and whitespace
        cleaned = str(price_str).replace("$", "").replace("€", "").replace("£", "").replace(",", "").strip()
        
        # Try to extract numeric value (handle cases like "$29.99" or "29.99")
        match = re.search(r'(\d+\.?\d*)', cleaned)
        if match:
            return float(match.group(1))
        
        return 0.00
    except Exception:
        return 0.00

def clean_title(title):
    
    # Remove emojis (4-byte UTF-8 characters)
    emoji_pattern = re.compile(
        "["
        "\U0001F600-\U0001F64F"  # emoticons
        "\U0001F300-\U0001F5FF"  # symbols & pictographs
        "\U0001F680-\U0001F6FF"  # transport & map symbols
        "\U0001F1E0-\U0001F1FF"  # flags (iOS)
        "\U00002702-\U000027B0"
        "\U000024C2-\U0001F251"
        "\U0001F900-\U0001F9FF"  # Supplemental Symbols and Pictographs
        "\U0001FA00-\U0001FA6F"  # Chess Symbols
        "\U0001FA70-\U0001FAFF"  # Symbols and Pictographs Extended-A
        "]+", 
        flags=re.UNICODE
    )
    cleaned = emoji_pattern.sub('', title)
    # Remove any remaining 4-byte UTF-8 characters
    cleaned = cleaned.encode('utf-8', 'ignore').decode('utf-8')
    cleaned = cleaned.strip()
    
    # Truncate to 255 characters (database column limit)
    if len(cleaned) > 255:
        cleaned = cleaned[:255]
    
    return cleaned


def normalize_steam_title(title: str) -> str:
    """Normalize titles for cross-source matching (Steam vs IGDB/affiliate)."""
    return normalize_match_title(title)


def _extract_numbers(text: str) -> set[str]:
    return set(re.findall(r"\d+", text or ""))


def _best_fallback_item_id(normalized_title: str, token_index: dict[str, list[tuple[int, str]]]) -> int | None:
    """Pick best candidate for a title that does not have exact normalized match."""
    if not normalized_title:
        return None

    tokens = [t for t in normalized_title.split() if len(t) > 1]
    if not tokens:
        return None

    # Search only candidates sharing the first meaningful token.
    candidates = token_index.get(tokens[0], [])
    if not candidates:
        return None

    title_numbers = _extract_numbers(normalized_title)
    best_score = -1.0
    best_item_id = None

    token_set = set(tokens)
    for item_id, item_norm in candidates:
        item_numbers = _extract_numbers(item_norm)
        if title_numbers and title_numbers != item_numbers:
            continue

        item_tokens = set(item_norm.split())
        if not item_tokens:
            continue

        overlap = len(token_set & item_tokens)
        union = len(token_set | item_tokens)
        jaccard = (overlap / union) if union else 0.0

        score = jaccard
        if item_norm.startswith(normalized_title) or normalized_title.startswith(item_norm):
            score += 0.15
        elif normalized_title in item_norm or item_norm in normalized_title:
            score += 0.1

        if score > best_score:
            best_score = score
            best_item_id = item_id

    # Keep fallback conservative to avoid incorrect matches.
    return best_item_id if best_score >= 0.7 else None


def build_item_title_lookup() -> tuple[dict[str, int], dict[str, list[tuple[int, str]]]]:
    """Build exact and token-based lookup for item title matching."""
    exact_lookup: dict[str, int] = {}
    token_index: dict[str, list[tuple[int, str]]] = {}

    with get_session() as session:
        rows = session.exec(select(Item.id, Item.norm_title, Item.title)).all()

    for item_id, item_norm_title, item_title in rows:
        if not item_id or not item_title:
            continue
        normalized = item_norm_title or normalize_steam_title(item_title)
        if not normalized:
            continue

        # Keep first seen item for deterministic mapping.
        exact_lookup.setdefault(normalized, item_id)

        first_token = normalized.split()[0] if normalized.split() else ""
        if first_token:
            token_index.setdefault(first_token, []).append((item_id, normalized))

    return exact_lookup, token_index


def reconcile_topsellers_item_ids(only_null: bool = True) -> int:
    """Backfill topsellers.item_id using the latest items table and title matching."""
    exact_lookup, token_index = build_item_title_lookup()
    updated = 0

    with get_session() as session:
        statement = select(TopSeller)
        if only_null:
            statement = statement.where(TopSeller.item_id.is_(None))

        rows = session.exec(statement).all()
        for row in rows:
            normalized_title = normalize_steam_title(row.title)
            row.norm_title = normalized_title
            item_id = exact_lookup.get(normalized_title)
            if item_id is None:
                item_id = _best_fallback_item_id(normalized_title, token_index)

            if item_id is not None and row.item_id != item_id:
                row.item_id = item_id
                updated += 1

        if updated:
            session.commit()

    logger.info(f"Reconciled topsellers item links: {updated} row(s) updated")
    return updated


def save_to_database(games):
    try:
        insert_values = []
        skipped_free = 0
        ranking = 1
        matched_count = 0
        exact_lookup, token_index = build_item_title_lookup()

        for game in games:
            cleaned_title = clean_title(game['title'])
            if not cleaned_title:
                continue

            price_str = game.get('price', 'N/A')
            if price_str in ["Free", "N/A", "?", ""]:
                skipped_free += 1
                logger.warning(f"Skipping game with invalid/free price: '{cleaned_title}' (price: '{price_str}')")
                continue

            price_value = parse_price(price_str)
            if price_value == 0.00:
                skipped_free += 1
                logger.warning(f"Skipping game with 0.00 price: '{cleaned_title}' (original: '{price_str}')")
                continue

            normalized_title = normalize_steam_title(cleaned_title)
            item_id = exact_lookup.get(normalized_title)
            if item_id is None:
                item_id = _best_fallback_item_id(normalized_title, token_index)

            if item_id is not None:
                matched_count += 1

            insert_values.append((ranking, cleaned_title, normalized_title, price_value, item_id))
            ranking += 1

        if not insert_values:
            logger.error("No valid games to insert, keeping existing topsellers data")
            return False

        inserted, skipped = insert_topsellers(insert_values)
        if skipped_free > 0:
            logger.warning(f"Skipped {skipped_free} games with free/invalid prices")
        logger.info(f"Successfully inserted {inserted} top sellers into database")
        if skipped > 0:
            logger.warning(f"Skipped {skipped} topsellers with validation errors")
        logger.info(f"Matched {matched_count}/{inserted} topsellers to items by normalized title")
        return True

    except Exception as e:
        logger.error(f"Error saving to database: {e}")
        return False


def fetch_and_save_steam_topsellers(cc=None, lang=None):
    cc = cc or config.STEAM_REGION
    lang = lang or config.STEAM_LANGUAGE
    logger.info("Starting Steam top sellers fetch and save")
    games = fetch_top500_topsellers(cc=cc, lang=lang)
    if not games:
        logger.error("Failed to fetch Steam top sellers")
        return False
    success = save_to_database(games)
    if success:
        logger.info("Steam top sellers fetch and save completed successfully")
    return success
