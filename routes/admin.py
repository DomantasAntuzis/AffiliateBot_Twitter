"""
Admin API endpoints for managing offers
"""
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import Optional
import mysql.connector
from database.db_connect import get_connection
from routes.auth import require_admin

admin_router = APIRouter()

class OfferUpdateRequest(BaseModel):
    affiliate_url: Optional[str] = None
    image_url: Optional[str] = None
    list_price: Optional[float] = None
    sale_price: Optional[float] = None
    discount: Optional[int] = None
    is_valid: Optional[bool] = None

def get_db():
    """Dependency for database connection"""
    connection = get_connection()
    if connection is None:
        raise HTTPException(status_code=500, detail="Database connection failed")
    return connection

@admin_router.put("/offers/{offer_id}")
async def update_offer(
    offer_id: int,
    update_data: OfferUpdateRequest,
    current_user: dict = Depends(require_admin),
    db = Depends(get_db)
):
    """
    Update an offer (admin only)
    
    Args:
        offer_id: ID of the offer to update
        update_data: Fields to update
        current_user: Current admin user (from auth)
        db: Database connection
    
    Returns:
        Updated offer data
    """
    try:
        cursor = db.cursor(dictionary=True)
        
        # Build update query dynamically based on provided fields
        update_fields = []
        params = []
        
        if update_data.affiliate_url is not None:
            update_fields.append("affiliate_url = %s")
            params.append(update_data.affiliate_url)
        
        if update_data.image_url is not None:
            update_fields.append("image_url = %s")
            params.append(update_data.image_url)
        
        if update_data.list_price is not None:
            update_fields.append("list_price = %s")
            params.append(update_data.list_price)
        
        if update_data.sale_price is not None:
            update_fields.append("sale_price = %s")
            params.append(update_data.sale_price)
        
        if update_data.discount is not None:
            update_fields.append("discount = %s")
            params.append(update_data.discount)
        
        if update_data.is_valid is not None:
            update_fields.append("is_valid = %s")
            params.append(1 if update_data.is_valid else 0)
        
        if not update_fields:
            raise HTTPException(status_code=400, detail="No fields to update")
        
        # Mark as manually edited to protect from automated updates
        update_fields.append("is_manually_edited = 1")
        
        # Update the offer
        update_query = f"UPDATE offers SET {', '.join(update_fields)} WHERE id = %s"
        params.append(offer_id)
        
        cursor.execute(update_query, params)
        db.commit()
        
        # Fetch updated offer
        cursor.execute("""
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
            WHERE o.id = %s
        """, (offer_id,))
        
        updated_offer = cursor.fetchone()
        cursor.close()
        
        if not updated_offer:
            raise HTTPException(status_code=404, detail="Offer not found")
        
        return updated_offer
        
    except mysql.connector.Error as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")

@admin_router.delete("/offers/{offer_id}")
async def delete_offer(
    offer_id: int,
    current_user: dict = Depends(require_admin),
    db = Depends(get_db)
):
    """
    Delete an offer (admin only)
    
    Args:
        offer_id: ID of the offer to delete
        current_user: Current admin user (from auth)
        db: Database connection
    
    Returns:
        Success message
    """
    try:
        cursor = db.cursor()
        
        # Check if offer exists
        cursor.execute("SELECT id FROM offers WHERE id = %s", (offer_id,))
        if not cursor.fetchone():
            cursor.close()
            raise HTTPException(status_code=404, detail="Offer not found")
        
        # Soft delete: mark as hidden instead of actually deleting
        # This prevents daily collection from re-inserting it
        cursor.execute("UPDATE offers SET is_hidden = 1 WHERE id = %s", (offer_id,))
        db.commit()
        cursor.close()
        
        return {"message": "Offer deleted successfully", "offer_id": offer_id}
        
    except mysql.connector.Error as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")

