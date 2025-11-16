"""
Items API endpoints
"""
from fastapi import APIRouter, Depends, HTTPException
import mysql.connector
import random
from database.db_connect import get_connection
from typing import Generator

router = APIRouter()

def get_db() -> Generator:
    """Dependency for database connection"""
    connection = get_connection()
    try:
        yield connection
    finally:
        if connection and connection.is_connected():
            connection.close()

@router.get("/")
async def get_offers(
    db = Depends(get_db)
):
    """
    Get all offers from the offers table
    
    Args:
        db: Database connection dependency
    
    Returns:
        List of all offers with item and distributor details
    """
    if db is None:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        cursor = db.cursor(dictionary=True)
        
        # Query offers with joins to get item title and distributor name
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
            ORDER BY o.id ASC
        """
        
        cursor.execute(query)
        offers = cursor.fetchall()
        cursor.close()
        
        # Shuffle the offers before returning
        random.shuffle(offers)
        
        return offers
        
    except mysql.connector.Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")

