from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse
from services.image_cache_service import (
    get_local_image_path, 
    download_igdb_image,
    get_image_stats
)

router = APIRouter()

@router.get("/igdb-images/{image_id}.jpg")
async def serve_igdb_image(image_id: str):
    """
    Serve IGDB image from local cache, or download if not cached
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
    """
    return get_image_stats()

