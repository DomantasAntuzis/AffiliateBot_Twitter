"""
Image serving routes for IGDB cached images
"""
from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse
from pathlib import Path
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.image_cache_service import (
    get_local_image_path, 
    is_image_cached, 
    download_igdb_image,
    get_image_stats
)
from utils.logger import logger

router = APIRouter()

@router.get("/igdb-images/{image_id}.jpg")
async def serve_igdb_image(image_id: str):
    """
    Serve IGDB image from local cache, or download if not cached
    
    Args:
        image_id: IGDB image ID (alphanumeric string)
    
    Returns:
        FileResponse: Image file
    """
    if not image_id:
        raise HTTPException(status_code=400, detail="Image ID is required")
    
    # Check if image is cached
    local_path = get_local_image_path(image_id)
    
    if not local_path.exists():
        # Try to download on-the-fly
        downloaded_path = download_igdb_image(image_id)
        if not downloaded_path:
            raise HTTPException(status_code=404, detail=f"Image {image_id} not found and could not be downloaded")
        local_path = downloaded_path
    
    # Serve the cached image
    if not local_path.exists():
        raise HTTPException(status_code=404, detail=f"Image {image_id} not found")
    
    return FileResponse(
        path=str(local_path),
        media_type="image/jpeg",
        headers={
            "Cache-Control": "public, max-age=31536000",  # Cache for 1 year
        }
    )

@router.get("/igdb-images/stats")
async def get_stats():
    """
    Get statistics about cached IGDB images
    
    Returns:
        dict: Image cache statistics
    """
    return get_image_stats()

