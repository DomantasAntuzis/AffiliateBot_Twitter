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
                d.name as distributor_name
            FROM offers o
            LEFT JOIN items i ON o.item_id = i.id
            LEFT JOIN distributors d ON o.distributor_id = d.id
        """
        
        # Add JOIN for genre filtering if needed
        where_clauses = []
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
        
        # Add WHERE clause if any filters are present
        if where_clauses:
            query += " WHERE " + " AND ".join(where_clauses)
        
        # Add sorting
        if sort_by == "discount_desc":
            query += " ORDER BY o.discount DESC, o.id ASC"
        elif sort_by == "discount_asc":
            query += " ORDER BY o.discount ASC, o.id ASC"
        else:
            query += " ORDER BY o.id ASC"
        
        query += " LIMIT %s OFFSET %s"
        
        cursor.execute(query, params + [limit, offset])
        offers = cursor.fetchall()
        cursor.close()
        
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
                d.name as distributor_name,
                ts.id as topseller_rank
            FROM offers o
            INNER JOIN items i ON o.item_id = i.id
            INNER JOIN distributors d ON o.distributor_id = d.id
            INNER JOIN topsellers ts ON i.title = ts.title
        """
        
        params = []
        where_clauses = ["o.is_valid = 1"]
        
        # Add genre filter if provided
        if genre and len(genre) > 0:
            query += " INNER JOIN item_genres ig ON o.item_id = ig.item_id"
            placeholders = ','.join(['%s'] * len(genre))
            where_clauses.append(f"ig.genre_id IN ({placeholders})")
            params.extend(genre)
        
        query += " WHERE " + " AND ".join(where_clauses)
        query += " ORDER BY ts.id ASC, d.name ASC LIMIT %s OFFSET %s"
        
        cursor.execute(query, params + [limit, offset])
        offers = cursor.fetchall()
        cursor.close()
        
        return offers
        
    except mysql.connector.Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")

