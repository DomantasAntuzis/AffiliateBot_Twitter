"""
IGDB Image Cache Service
Handles downloading and caching IGDB cover images locally
"""
import os
import sys
import requests
from pathlib import Path
from typing import Optional

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import config as config
from utils.logger import logger

# IGDB images directory
IMAGES_DIR = Path(config.DATA_DIR) / "igdb_images"
IMAGES_DIR.mkdir(parents=True, exist_ok=True)

def get_local_image_path(image_id: str) -> Path:
    return IMAGES_DIR / f"{image_id}.jpg"

def is_image_cached(image_id: str) -> bool:
    if not image_id:
        return False
    return get_local_image_path(image_id).exists()

def download_igdb_image(image_id: str, size: str = '720p') -> Optional[Path]:
    if not image_id:
        logger.warning("No image_id provided for download")
        return None
    
    # Check if already cached
    local_path = get_local_image_path(image_id)
    if local_path.exists():
        logger.debug(f"Image {image_id} already cached")
        return local_path
    
    # Get IGDB image URL
    image_url = f"https://images.igdb.com/igdb/image/upload/t_{size}/{image_id}.jpg"
    
    try:
        logger.info(f"Downloading IGDB image {image_id} from {image_url}")
        
        # Download image
        response = requests.get(image_url, timeout=30, stream=True)
        response.raise_for_status()
        
        # Save to local file
        with open(local_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        logger.info(f"Successfully downloaded image {image_id} to {local_path}")
        return local_path
        
    except requests.RequestException as e:
        logger.error(f"Failed to download image {image_id} from {image_url}: {e}")
        # Clean up partial file if exists
        if local_path.exists():
            local_path.unlink()
        return None
    except Exception as e:
        logger.error(f"Unexpected error downloading image {image_id}: {e}")
        if local_path.exists():
            local_path.unlink()
        return None

def get_image_stats() -> dict:
    images = list(IMAGES_DIR.glob("*.jpg"))
    total_size = sum(img.stat().st_size for img in images)
    
    return {
        "total_images": len(images),
        "total_size_mb": round(total_size / (1024 * 1024), 2),
        "cache_dir": str(IMAGES_DIR)
    }

