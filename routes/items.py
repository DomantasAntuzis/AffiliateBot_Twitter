"""
Items API endpoints
"""
from fastapi import APIRouter, Depends, HTTPException, Query
import mysql.connector
import random
import re
from database.db_connect import get_connection
from typing import Generator, Optional, List

router = APIRouter()

def _normalize_title_for_matching(title: str) -> str:
    """
    Normalize title for fuzzy matching (same logic as in affiliate_service.py)
    Removes punctuation and normalizes whitespace
    """
    # Convert to lowercase
    normalized = title.lower().strip()
    
    # Replace common punctuation with spaces (hyphens, colons, semicolons, etc.)
    normalized = re.sub(r'[-:;–—]', ' ', normalized)
    
    # Remove other punctuation (keep apostrophes for names like "O'Brien")
    normalized = re.sub(r'[^\w\s\']', '', normalized)
    
    # Collapse multiple spaces to single space
    normalized = re.sub(r'\s+', ' ', normalized)
    
    # Trim
    normalized = normalized.strip()
    
    return normalized

def get_db() -> Generator:
    """Dependency for database connection"""
    connection = get_connection()
    try:
        yield connection
    finally:
        if connection and connection.is_connected():
            connection.close()

@router.get("/offers_list")
async def get_offers(
    distributor: Optional[List[str]] = Query(None, description="Filter by distributor name(s). Example: ?distributor=GOG&distributor=YUPLAY"),
    sort_by_top_sellers: bool = Query(False, description="Sort by Steam top 500 sellers (top sellers first)"),
    db = Depends(get_db)
):
    """
    Get all offers from the offers table with optional filtering and sorting
    
    Args:
        distributor: Optional list of distributor names to filter by (e.g., "GOG", "YUPLAY", "GamersGate", "IndieGala")
        sort_by_top_sellers: If True, prioritize offers matching Steam's top 500 selling games
        db: Database connection dependency
    
    Returns:
        List of offers matching the filters with item and distributor details
        
    Examples:
        - Get all offers: /api/offers_list
        - Get only GOG offers: /api/offers_list?distributor=GOG
        - Get GOG and YUPLAY offers: /api/offers_list?distributor=GOG&distributor=YUPLAY
        - Get all offers sorted by top sellers: /api/offers_list?sort_by_top_sellers=true
        - Get GOG offers sorted by top sellers: /api/offers_list?distributor=GOG&sort_by_top_sellers=true
    """
    if db is None:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cursor = db.cursor(dictionary=True)
        
        # Build query with optional distributor filter
        query = """
            SELECT 
                o.id,
                o.item_id,
                o.distributor_id,
                o.affiliate_url,
                o.image_url,
                o.list_price,
                o.sale_price,
                o.discount,
                o.is_valid,
                i.title as item_title,
                d.name as distributor_name
            FROM offers o
            LEFT JOIN items i ON o.item_id = i.id
            LEFT JOIN distributors d ON o.distributor_id = d.id
        """
        
        # Add WHERE clause if distributor filter is provided
        params = []
        if distributor and len(distributor) > 0:
            # Create placeholders for IN clause
            placeholders = ','.join(['%s'] * len(distributor))
            query += f" WHERE d.name IN ({placeholders})"
            params.extend(distributor)
        
        query += " ORDER BY o.id ASC"
        
        cursor.execute(query, params)
        offers = cursor.fetchall()
        cursor.close()
        
        # Sort by top sellers if requested
        if sort_by_top_sellers:
            # Load Steam top sellers from database
            topsellers_cursor = db.cursor(dictionary=True)
            topsellers_cursor.execute("SELECT title FROM topsellers ORDER BY id ASC")
            topsellers_rows = topsellers_cursor.fetchall()
            topsellers_cursor.close()
            
            # Create a set of normalized top seller titles for fast lookup
            steam_top_sellers = set()
            for row in topsellers_rows:
                normalized_title = _normalize_title_for_matching(row['title'])
                steam_top_sellers.add(normalized_title)
            
            # Separate offers into top sellers and others
            top_seller_offers = []
            other_offers = []
            
            for offer in offers:
                item_title = offer.get('item_title', '')
                if item_title:
                    normalized_title = _normalize_title_for_matching(item_title)
                    if normalized_title in steam_top_sellers:
                        top_seller_offers.append(offer)
                    else:
                        other_offers.append(offer)
                else:
                    other_offers.append(offer)
            
            # Shuffle each group separately for variety
            random.shuffle(top_seller_offers)
            random.shuffle(other_offers)
            
            # Combine: top sellers first, then others
            offers = top_seller_offers + other_offers
        else:
            # Shuffle all offers randomly
            random.shuffle(offers)
        
        return offers
        
    except mysql.connector.Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")

