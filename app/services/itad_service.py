"""
IsThereAnyDeal (ITAD) service

Provides live price data for affiliate offers by querying the
IsThereAnyDeal API (https://api.isthereanydeal.com).

Supported store IDs used in this project:
  GamersGate   = 24
  GOG          = 35
  IndieGala    = 42

API docs: https://docs.isthereanydeal.com/
Register for a key: https://isthereanydeal.com/dev/app/
"""

import time
import requests
from urllib.parse import urlparse, urlencode, parse_qs, urlunparse, urljoin

import config
from utils.logger import logger


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ITAD_BASE_URL = "https://api.isthereanydeal.com"

# Store IDs as listed on ITAD – keep in sync with user-specified values
ITAD_SHOP_IDS = [24, 35, 42]  # GamersGate, GOG, IndieGala Store

# Map CJ / scraper distributor names to ITAD shop IDs
DISTRIBUTOR_TO_SHOP_ID: dict[str, int] = {
    "GamersGate.com": 24,
    "GOG.COM INT": 35,
    "IndieGala": 42,
}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _api_key() -> str | None:
    key = getattr(config, "ITAD_API_KEY", None)
    if not key or key == "YOUR_ITAD_API_KEY_HERE":
        return None
    return key


# ---------------------------------------------------------------------------
# Public: title → ITAD UUID lookup
# ---------------------------------------------------------------------------

def lookup_itad_ids(titles: list[str]) -> dict[str, str]:
    """
    Look up ITAD game UUIDs for a batch of game titles.

    Uses POST /lookup/id/title/v1 which accepts a JSON array of titles and
    returns a {title: uuid} map in a single request.  Much faster than
    calling the per-title search endpoint in a loop.
    """
    key = _api_key()
    if not key:
        logger.warning("ITAD: ITAD_API_KEY not configured – skipping lookup")
        return {}

    if not titles:
        return {}

    session = requests.Session()
    results: dict[str, str] = {}

    try:
        # The endpoint accepts up to 200 titles per call
        chunk_size = 200
        for i in range(0, len(titles), chunk_size):
            chunk = titles[i : i + chunk_size]
            try:
                resp = session.post(
                    f"{ITAD_BASE_URL}/lookup/id/title/v1",
                    json=chunk,
                    params={"key": key},
                    timeout=20,
                )

                if resp.status_code == 429:
                    logger.warning("ITAD: rate-limit hit on lookup – sleeping 3 s")
                    time.sleep(3)
                    resp = session.post(
                        f"{ITAD_BASE_URL}/lookup/id/title/v1",
                        json=chunk,
                        params={"key": key},
                        timeout=20,
                    )

                if resp.status_code != 200:
                    logger.warning(f"ITAD lookup returned {resp.status_code}: {resp.text[:200]}")
                    continue

                data = resp.json()
                # Response: {"Game Title": "uuid-or-null", ...}
                for title, uuid in data.items():
                    if uuid:
                        results[title] = uuid

            except Exception as exc:
                logger.warning(f"ITAD lookup chunk error: {exc}")

    finally:
        session.close()

    logger.info(f"ITAD: resolved {len(results)}/{len(titles)} game IDs")
    return results


# ---------------------------------------------------------------------------
# Public: UUID list → live prices
# ---------------------------------------------------------------------------

def fetch_live_prices(
    itad_ids: list[str],
    country: str | None = None,
    shop_ids: list[int] | None = None,
) -> dict[str, dict[int, dict]]:
    """
    Fetch current prices from ITAD for the given game UUIDs.

    Uses POST /games/prices/v3 (up to 100 IDs per request).

    Returns dict:
        itad_uuid -> {
            shop_id (int): {
                "price":   float,   # current sale price
                "regular": float,   # full/regular price
                "cut":     int,     # discount percentage (0-100)
                "url":     str,     # direct store URL
            }
        }
    """
    key = _api_key()
    if not key:
        logger.warning("ITAD: ITAD_API_KEY not configured – skipping price fetch")
        return {}

    if not itad_ids:
        return {}

    if country is None:
        country = getattr(config, "ITAD_COUNTRY", "US")
    if shop_ids is None:
        shop_ids = ITAD_SHOP_IDS

    session = requests.Session()
    all_prices: dict[str, dict[int, dict]] = {}

    try:
        chunk_size = 100
        for i in range(0, len(itad_ids), chunk_size):
            chunk = itad_ids[i : i + chunk_size]
            try:
                resp = session.post(
                    f"{ITAD_BASE_URL}/games/prices/v3",
                    json=chunk,
                    params={
                        "key": key,
                        "country": country,
                        "shops": ",".join(str(s) for s in shop_ids),
                    },
                    timeout=20,
                )

                if resp.status_code == 429:
                    logger.warning("ITAD: rate-limit hit on prices – sleeping 5 s")
                    time.sleep(5)
                    resp = session.post(
                        f"{ITAD_BASE_URL}/games/prices/v3",
                        json=chunk,
                        params={
                            "key": key,
                            "country": country,
                            "shops": ",".join(str(s) for s in shop_ids),
                        },
                        timeout=20,
                    )

                if resp.status_code != 200:
                    logger.warning(
                        f"ITAD prices API returned {resp.status_code}: {resp.text[:200]}"
                    )
                    continue

                for entry in resp.json():
                    game_id = entry.get("id")
                    if not game_id:
                        continue

                    shop_prices: dict[int, dict] = {}
                    for deal in entry.get("deals", []):
                        shop_id = deal.get("shop", {}).get("id")
                        price_block = deal.get("price") or {}
                        regular_block = deal.get("regular") or {}
                        if shop_id is not None:
                            shop_prices[int(shop_id)] = {
                                "price": price_block.get("amount"),
                                "regular": regular_block.get("amount"),
                                "cut": deal.get("cut"),
                                "url": deal.get("url"),
                            }

                    all_prices[game_id] = shop_prices

            except Exception as exc:
                logger.warning(f"ITAD prices chunk error: {exc}")

    finally:
        session.close()

    logger.info(f"ITAD: received prices for {len(all_prices)} games")
    return all_prices


# ---------------------------------------------------------------------------
# Public: enrich flat deal list with ITAD live prices
# ---------------------------------------------------------------------------

def enrich_deals_with_itad(deals: list[dict]) -> list[dict]:
    """
    Enrich a flat list of deal dicts with verified live prices from ITAD.

    Expected keys per deal: 'title', 'source' (distributor name),
    'price', 'salePrice', 'discount'.

    Only deals for stores that have an ITAD shop mapping are updated.
    When ITAD data is found it overwrites price / salePrice / discount in-place.

    Returns the same list (mutated).
    """
    if not deals:
        return deals

    # Only try to enrich deals for stores we have ITAD shop IDs for
    enrichable = [d for d in deals if d.get("source") in DISTRIBUTOR_TO_SHOP_ID]
    if not enrichable:
        logger.info("ITAD: no enrichable deals (no matching shop IDs)")
        return deals

    unique_titles = list({d["title"] for d in enrichable})
    logger.info(f"ITAD: looking up IDs for {len(unique_titles)} unique titles…")
    title_to_itad = lookup_itad_ids(unique_titles)

    if not title_to_itad:
        logger.warning("ITAD: could not resolve any game IDs – prices unchanged")
        return deals

    itad_ids = list(title_to_itad.values())
    logger.info(f"ITAD: fetching live prices for {len(itad_ids)} games…")
    prices = fetch_live_prices(itad_ids)

    updated = 0
    for deal in enrichable:
        title = deal.get("title", "")
        source = deal.get("source", "")
        shop_id = DISTRIBUTOR_TO_SHOP_ID.get(source)
        if not shop_id:
            continue

        itad_id = title_to_itad.get(title)
        if not itad_id:
            continue

        shop_data = prices.get(itad_id, {}).get(shop_id)
        if not shop_data:
            continue

        itad_sale = shop_data.get("price")
        itad_regular = shop_data.get("regular")
        itad_cut = shop_data.get("cut")

        # Only override when ITAD has both price points
        if itad_sale is not None and itad_regular is not None and itad_regular > 0:
            deal["price"] = round(float(itad_regular), 2)
            deal["salePrice"] = round(float(itad_sale), 2)
            if itad_cut is not None:
                deal["discount"] = int(itad_cut)
            else:
                deal["discount"] = round(
                    ((itad_regular - itad_sale) / itad_regular) * 100
                )
            updated += 1

    logger.info(f"ITAD: enriched {updated}/{len(enrichable)} deals with live prices")
    return deals


# ---------------------------------------------------------------------------
# Public: fetch all current GamersGate deals from ITAD
# ---------------------------------------------------------------------------

_GAMERSGATE_SHOP_ID = 24


def fetch_gamersgate_deals(country: str | None = None, currency: str | None = None) -> list[dict]:
    """
    Fetch all current GamersGate deals via ITAD's /deals/v2 endpoint.

    Returns a minimal list of dicts:
        {title, price, sale_price, discount, store_url}

    Callers (affiliate_service) are responsible for matching these against
    CJ affiliate URLs by title, since the CJ affiliate link is required for
    commission tracking.

    Falls back to an empty list when ITAD_API_KEY is not configured.
    """
    key = _api_key()
    if not key:
        logger.warning("ITAD: ITAD_API_KEY not configured – GamersGate list will be empty")
        return []

    if country is None:
        country = getattr(config, "ITAD_COUNTRY", "US")

    session = requests.Session()
    deals: list[dict] = []
    offset = 0
    limit = 200

    try:
        while True:
            try:
                resp = session.get(
                    f"{ITAD_BASE_URL}/deals/v2",
                    params={
                        "key": key,
                        "shops": _GAMERSGATE_SHOP_ID,
                        "country": country,
                        "limit": limit,
                        "offset": offset,
                        "sort": "-cut",
                    },
                    timeout=20,
                )

                if resp.status_code == 429:
                    logger.warning("ITAD: rate-limit on GG deals – sleeping 5 s")
                    time.sleep(5)
                    continue

                if resp.status_code != 200:
                    logger.warning(
                        f"ITAD GG deals API returned {resp.status_code}: {resp.text[:200]}"
                    )
                    break

                data = resp.json()
                entries = data.get("list", [])
                if not entries:
                    break

                for entry in entries:
                    try:
                        title = (entry.get("title") or "").strip()
                        if not title:
                            continue

                        deal = entry.get("deal") or {}
                        price_block = deal.get("price") or {}
                        regular_block = deal.get("regular") or {}
                        cut = deal.get("cut") or 0
                        store_url = (deal.get("url") or "").strip()

                        sale_amount = price_block.get("amount")
                        regular_amount = regular_block.get("amount")

                        if not sale_amount or not store_url:
                            continue

                        list_price = regular_amount if regular_amount else sale_amount

                        deals.append({
                            "title": title,
                            "price": str(round(float(list_price), 2)),
                            "sale_price": str(round(float(sale_amount), 2)),
                            "discount": str(int(cut)) if cut else "",
                            "store_url": store_url,
                        })

                    except Exception as exc:
                        logger.debug(f"ITAD: skipping GG entry – {exc}")

                if not data.get("hasMore", False):
                    break

                offset = data.get("nextOffset", offset + len(entries))

            except Exception as exc:
                logger.warning(f"ITAD GamersGate deals page error: {exc}")
                break

    finally:
        session.close()

    logger.info(f"ITAD: fetched {len(deals)} GamersGate deals")
    return deals


_GOG_SHOP_ID = 35


def fetch_gog_deals(country: str, currency: str) -> list[dict]:
    """
    Fetch all current GOG deals via ITAD's /deals/v2 endpoint.
    """
    key = _api_key()
    if not key:
        logger.warning("ITAD: ITAD_API_KEY not configured – GOG list will be empty")
        return []

    if country is None:
        country = getattr(config, "ITAD_COUNTRY", "US")

    session = requests.Session()
    deals: list[dict] = []
    offset = 0
    limit = 200

    try:
        while True:
            try:
                resp = session.get(
                    f"{ITAD_BASE_URL}/deals/v2",
                    params={
                        "key": key,
                        "shops": _GOG_SHOP_ID,
                        "country": country,
                        "limit": limit,
                        "offset": offset,
                        "sort": "-cut",
                    },
                    timeout=20,
                )

                if resp.status_code == 429:
                    logger.warning("ITAD: rate-limit on GOG deals – sleeping 5 s")
                    time.sleep(5)
                    continue

                if resp.status_code != 200:
                    logger.warning(
                        f"ITAD GOG deals API returned {resp.status_code}: {resp.text[:200]}"
                    )
                    break

                data = resp.json()
                entries = data.get("list", [])
                if not entries:
                    break

                for entry in entries:
                    try:
                        title = (entry.get("title") or "").strip()
                        if not title:
                            continue

                        deal = entry.get("deal") or {}
                        price_block = deal.get("price") or {}
                        regular_block = deal.get("regular") or {}
                        cut = deal.get("cut") or 0
                        store_url = (deal.get("url") or "").strip()

                        sale_amount = price_block.get("amount")
                        regular_amount = regular_block.get("amount")

                        if not sale_amount or not store_url:
                            continue

                        list_price = regular_amount if regular_amount else sale_amount

                        deals.append({
                            "title": title,
                            "price": str(round(float(list_price), 2)),
                            "sale_price": str(round(float(sale_amount), 2)),
                            "discount": str(int(cut)) if cut else "",
                            "store_url": store_url,
                        })

                    except Exception as exc:
                        logger.debug(f"ITAD: skipping GOG entry – {exc}")

                if not data.get("hasMore", False):
                    break

                offset = data.get("nextOffset", offset + len(entries))

            except Exception as exc:
                logger.warning(f"ITAD GOG deals page error: {exc}")
                break

    finally:
        session.close()

    logger.info(f"ITAD: fetched {len(deals)} GOG deals")
    return deals


# ---------------------------------------------------------------------------
# Public: fetch all current IndieGala deals (replaces Selenium scraping)
# ---------------------------------------------------------------------------

_INDIEGALA_SHOP_ID = 42
_INDIEGALA_AFFILIATE_REF = "mzvkywq"

# Preferred asset keys in order of preference
_ASSET_KEYS = ("banner400", "banner600", "banner300", "banner145")


def _add_affiliate_ref(url: str) -> str:
    """Append ?ref=mzvkywq (or &ref=) to a store URL."""
    if not url:
        return url
    parsed = urlparse(url)
    qs = parse_qs(parsed.query, keep_blank_values=True)
    qs["ref"] = [_INDIEGALA_AFFILIATE_REF]
    new_query = urlencode(qs, doseq=True)
    return urlunparse(parsed._replace(query=new_query))


def fetch_indiegala_deals(country: str, currency: str) -> list[dict]:
    """
    Fetch all current IndieGala deals via ITAD's /deals/v2 endpoint.
    """
    key = _api_key()
    if not key:
        logger.warning("ITAD: ITAD_API_KEY not configured – IndieGala list will be empty")
        return []

    if country is None:
        country = getattr(config, "ITAD_COUNTRY", "US")

    session = requests.Session()
    products: list[dict] = []
    offset = 0
    limit = 200  # max allowed by ITAD API (integer [ 1 .. 200 ])

    try:
        while True:
            try:
                resp = session.get(
                    f"{ITAD_BASE_URL}/deals/v2",
                    params={
                        "key": key,
                        "shops": _INDIEGALA_SHOP_ID,
                        "country": country,
                        "limit": limit,
                        "offset": offset,
                        "sort": "-cut",   # ITAD sort: prefix '-' = descending (highest discount first)
                    },
                    timeout=20,
                )

                if resp.status_code == 429:
                    logger.warning("ITAD: rate-limit on deals endpoint – sleeping 5 s")
                    time.sleep(5)
                    continue

                if resp.status_code != 200:
                    logger.warning(
                        f"ITAD deals API returned {resp.status_code}: {resp.text[:200]}"
                    )
                    break

                data = resp.json()
                entries = data.get("list", [])
                if not entries:
                    break

                for entry in entries:
                    try:
                        title = (entry.get("title") or "").strip()
                        if not title:
                            continue

                        deal = entry.get("deal") or {}
                        price_block = deal.get("price") or {}
                        regular_block = deal.get("regular") or {}
                        cut = deal.get("cut") or 0
                        store_url = (deal.get("url") or "").strip()

                        sale_amount = price_block.get("amount")
                        regular_amount = regular_block.get("amount")

                        if not sale_amount or not store_url:
                            continue

                        # Use regular price as list price; fall back to sale if absent
                        list_price = regular_amount if regular_amount else sale_amount

                        # Pick best available banner image from ITAD assets
                        assets = entry.get("assets") or {}
                        image_url = ""
                        for key_name in _ASSET_KEYS:
                            candidate = assets.get(key_name, "")
                            if candidate:
                                image_url = candidate
                                break

                        products.append({
                            "PROGRAM_NAME": "IndieGala",
                            "ID": f"ITAD-IG-{(entry.get('id') or title.replace(' ', '-'))[:50]}",
                            "TITLE": title,
                            "LINK": _add_affiliate_ref(store_url),
                            "IMAGE_LINK": image_url,
                            "AVAILABILITY": "in stock",
                            "PRICE": str(round(float(list_price), 2)),
                            "SALE_PRICE": str(round(float(sale_amount), 2)),
                            "DISCOUNT": str(int(cut)) if cut else "",
                        })

                    except Exception as exc:
                        logger.debug(f"ITAD: skipping IndieGala entry – {exc}")

                if not data.get("hasMore", False):
                    break

                # Use the API-provided cursor for the next page
                offset = data.get("nextOffset", offset + len(entries))

            except Exception as exc:
                logger.warning(f"ITAD IndieGala deals page error: {exc}")
                break

    finally:
        session.close()

    logger.info(f"ITAD: fetched {len(products)} IndieGala deals")
    return products
