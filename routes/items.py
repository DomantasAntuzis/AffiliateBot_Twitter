"""
Items API endpoints
"""
from fastapi import APIRouter, Depends, HTTPException, Query
import mysql.connector
import re
import os
from database.db_connect import get_connection
from typing import Generator, Optional, List
from services.image_cache_service import is_image_cached

router = APIRouter()

# Get API base URL from environment or use default
API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8001")

def _transform_image_url(offer: dict) -> dict:
    """
    Transform image_url to use local IGDB image if available, otherwise keep original affiliate image
    
    Args:
        offer: Offer dictionary with image_url and igdb_cover_image_id
    
    Returns:
        dict: Modified offer with updated image_url
    """
    igdb_image_id = offer.get('igdb_cover_image_id')
    
    # Use local IGDB image if available and cached
    if igdb_image_id and igdb_image_id != '0' and igdb_image_id != '':
        igdb_image_id = str(igdb_image_id).strip()
        if is_image_cached(igdb_image_id):
            offer['image_url'] = f"{API_BASE_URL}/api/igdb-images/{igdb_image_id}.jpg"
    
    # Remove igdb_cover_image_id from response (internal use only)
    offer.pop('igdb_cover_image_id', None)
    
    return offer

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

@router.get("/genres")
async def get_genres(db = Depends(get_db)):
    """
    Get all available genres
    
    Returns:
        List of genres with id and name
    """
    if db is None:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cursor = db.cursor(dictionary=True)
        query = "SELECT id, name FROM genres ORDER BY name ASC"
        cursor.execute(query)
        genres = cursor.fetchall()
        cursor.close()
        return genres
    except mysql.connector.Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")

@router.get("/offers_list")
async def get_offers(
    distributor: Optional[List[str]] = Query(None, description="Filter by distributor name(s). Example: ?distributor=GOG&distributor=YUPLAY"),
    genre: Optional[List[int]] = Query(None, description="Filter by genre ID(s). Example: ?genre=1&genre=2"),
    sort_by: Optional[str] = Query(None, description="Sort by discount: 'discount_desc' or 'discount_asc'"),
    limit: int = Query(60, ge=1, le=200, description="Number of items to return"),
    offset: int = Query(0, ge=0, description="Number of items to skip"),
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
    """
    if db is None:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cursor = db.cursor(dictionary=True)
        
        # Build query with optional filters
        query = """
            SELECT DISTINCT
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
                i.igdb_cover_image_id,
                d.name as distributor_name
            FROM offers o
            LEFT JOIN items i ON o.item_id = i.id
            LEFT JOIN distributors d ON o.distributor_id = d.id
        """
        
        # Add JOIN for genre filtering if needed
        where_clauses = ["o.is_hidden = 0"]  # Always filter out hidden items
        params = []
        
        if genre and len(genre) > 0:
            query += " INNER JOIN item_genres ig ON o.item_id = ig.item_id"
            placeholders = ','.join(['%s'] * len(genre))
            where_clauses.append(f"ig.genre_id IN ({placeholders})")
            params.extend(genre)
        
        # Add distributor filter if provided
        if distributor and len(distributor) > 0:
            placeholders = ','.join(['%s'] * len(distributor))
            where_clauses.append(f"d.name IN ({placeholders})")
            params.extend(distributor)
        
        # Add WHERE clause (always includes is_hidden = 0)
        query += " WHERE " + " AND ".join(where_clauses)
        # Note: is_hidden = 0 is already in WHERE clause above
        
        # Add sorting
        if sort_by == "discount_desc":
            query += " ORDER BY o.discount DESC, o.id ASC"
        elif sort_by == "discount_asc":
            query += " ORDER BY o.discount ASC, o.id ASC"
        else:
            # Deterministic shuffle - mixes distributors and games evenly
            # Uses modulo hash for consistent ordering per day/session
            query += " ORDER BY MOD(o.id * 7919, 1000000), o.discount DESC, d.name"
        
        query += " LIMIT %s OFFSET %s"
        
        cursor.execute(query, params + [limit, offset])
        offers = cursor.fetchall()
        cursor.close()
        
        # Transform image URLs to use local IGDB images when available
        for offer in offers:
            _transform_image_url(offer)
        
        return offers
        
    except mysql.connector.Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")

@router.get("/topsellers")
async def get_topsellers(
    genre: Optional[List[int]] = Query(None, description="Filter by genre ID(s). Example: ?genre=1&genre=2"),
    limit: int = Query(60, ge=1, le=200, description="Number of items to return"),
    offset: int = Query(0, ge=0, description="Number of items to skip"),
    db = Depends(get_db)
):
    """
    Get offers for games that are in Steam's top 500 sellers list
    
    Args:
        distributor: Optional list of distributor names to filter by (e.g., "GOG", "YUPLAY", "GamersGate", "IndieGala")
        db: Database connection dependency
    
    Returns:
        List of offers for top seller games with item and distributor details
        
    Examples:
        - Get all top seller offers: /api/topsellers
        - Get only GOG top seller offers: /api/topsellers?distributor=GOG
        - Get GOG and YUPLAY top seller offers: /api/topsellers?distributor=GOG&distributor=YUPLAY
    """
    if db is None:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cursor = db.cursor(dictionary=True)
        
        # Query: match topsellers with offers
        query = """
            SELECT DISTINCT
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
                i.igdb_cover_image_id,
                d.name as distributor_name,
                ts.id as topseller_rank
            FROM offers o
            INNER JOIN items i ON o.item_id = i.id
            INNER JOIN distributors d ON o.distributor_id = d.id
            INNER JOIN topsellers ts ON i.title = ts.title
        """
        
        params = []
        where_clauses = ["o.is_valid = 1", "o.is_hidden = 0"]
        
        # Add genre filter if provided
        if genre and len(genre) > 0:
            query += " INNER JOIN item_genres ig ON o.item_id = ig.item_id"
            placeholders = ','.join(['%s'] * len(genre))
            where_clauses.append(f"ig.genre_id IN ({placeholders})")
            params.extend(genre)
        
        query += " WHERE " + " AND ".join(where_clauses)
        # Keep topseller ranking but shuffle distributors within each game group
        query += " ORDER BY ts.id ASC, MOD(o.id, 10), d.name LIMIT %s OFFSET %s"
        
        cursor.execute(query, params + [limit, offset])
        offers = cursor.fetchall()
        cursor.close()
        
        # Transform image URLs to use local IGDB images when available
        for offer in offers:
            _transform_image_url(offer)
        
        return offers
        
    except mysql.connector.Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")

@router.get("/search")
async def search_offers(
    q: str = Query("", description="Search query string"),
    distributor: Optional[List[str]] = Query(None, description="Filter by distributor name(s). Example: ?distributor=GOG"),
    genre: Optional[List[int]] = Query(None, description="Filter by genre ID(s). Example: ?genre=1&genre=2"),
    sort_by: Optional[str] = Query(None, description="Sort by discount: 'discount_desc' or 'discount_asc'"),
    limit: int = Query(10, ge=1, le=200, description="Number of items to return (default 10 for dropdown, up to 200 for search page)"),
    offset: int = Query(0, ge=0, description="Number of items to skip"),
    db = Depends(get_db)
):
    """
    Search offers by game title using fuzzy matching
    Uses normalized title matching for better results
    
    Args:
        q: Search query string (required)
        distributor: Optional distributor filter
        genre: Optional genre filter
        sort_by: Optional sort order
        limit: Number of results to return (default 10 for dropdown)
        offset: Pagination offset
        db: Database connection
    
    Returns:
        List of offers matching the search query
        
    Examples:
        - Search for "cyberpunk": /api/search?q=cyberpunk
        - Search with distributor filter: /api/search?q=witcher&distributor=GOG
    """
    if db is None:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    if not q or not q.strip():
        return []
    
    try:
        cursor = db.cursor(dictionary=True)
        
        # Normalize search query using the same normalization function
        normalized_query = _normalize_title_for_matching(q.strip())
        
        # Split into words for better fuzzy matching
        query_words = [w for w in normalized_query.split() if len(w) >= 2]
        
        if not query_words:
            return []
        
        # Build query with fuzzy matching
        query = """
            SELECT DISTINCT
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
                i.igdb_cover_image_id,
                d.name as distributor_name
            FROM offers o
            LEFT JOIN items i ON o.item_id = i.id
            LEFT JOIN distributors d ON o.distributor_id = d.id
        """
        
        where_clauses = ["o.is_hidden = 0"]  # Always filter out hidden items
        params = []
        
        # Add genre filter if needed
        if genre and len(genre) > 0:
            query += " INNER JOIN item_genres ig ON o.item_id = ig.item_id"
            placeholders = ','.join(['%s'] * len(genre))
            where_clauses.append(f"ig.genre_id IN ({placeholders})")
            params.extend(genre)
        
        # Build search condition: match normalized title
        # Create a pattern that matches the normalized query
        search_pattern = f"%{normalized_query}%"
        
        # Also match individual words for better fuzzy matching
        word_patterns = [f"%{word}%" for word in query_words[:5]]  # Limit to 5 words
        
        # Combine patterns: match full query OR individual words
        # Use normalized title matching in SQL (similar to Python normalization)
        search_conditions = [
            "LOWER(REPLACE(REPLACE(REPLACE(REPLACE(i.title, '-', ' '), ':', ' '), ';', ' '), '–', ' ')) LIKE %s"
        ]
        params.append(search_pattern)
        
        # Add individual word matches
        for word_pattern in word_patterns:
            search_conditions.append(
                "LOWER(REPLACE(REPLACE(REPLACE(REPLACE(i.title, '-', ' '), ':', ' '), ';', ' '), '–', ' ')) LIKE %s"
            )
            params.append(word_pattern)
        
        where_clauses.append(f"({' OR '.join(search_conditions)})")
        
        # Add distributor filter if provided
        if distributor and len(distributor) > 0:
            placeholders = ','.join(['%s'] * len(distributor))
            where_clauses.append(f"d.name IN ({placeholders})")
            params.extend(distributor)
        
        # Add WHERE clause
        if where_clauses:
            query += " WHERE " + " AND ".join(where_clauses)
        
        # Add sorting
        if sort_by == "discount_desc":
            query += " ORDER BY o.discount DESC, o.id ASC"
        elif sort_by == "discount_asc":
            query += " ORDER BY o.discount ASC, o.id ASC"
        else:
            # Default: prioritize exact matches, then by ID
            # Use parameterized query for safety
            query += " ORDER BY "
            query += f"CASE WHEN LOWER(REPLACE(REPLACE(REPLACE(REPLACE(i.title, '-', ' '), ':', ' '), ';', ' '), '–', ' ')) LIKE %s THEN 1 "
            query += f"WHEN LOWER(REPLACE(REPLACE(REPLACE(REPLACE(i.title, '-', ' '), ':', ' '), ';', ' '), '–', ' ')) LIKE %s THEN 2 "
            query += "ELSE 3 END, "
            query += "o.id ASC"
            # Add the patterns for ORDER BY (exact start match, then contains match)
            params.append(f"{normalized_query}%")  # Starts with query
            params.append(search_pattern)  # Contains query
        
        query += " LIMIT %s OFFSET %s"
        
        cursor.execute(query, params + [limit, offset])
        offers = cursor.fetchall()
        cursor.close()
        
        # Transform image URLs to use local IGDB images when available
        for offer in offers:
            _transform_image_url(offer)
        
        return offers
        
    except mysql.connector.Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")

