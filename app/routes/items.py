"""
Items API endpoints
"""
from fastapi import APIRouter, HTTPException, Query
import re
import os
from typing import Optional, List

from database.queries.items import (
  get_genres as db_get_genres,
  get_offers,
  get_topsellers as db_get_topsellers,
  search_offers as db_search_offers,
)
from services.image_cache_service import is_image_cached
from utils.helpers import _normalize_title

# Try to import Levenshtein for fuzzy matching, fallback to basic if not available
try:
  from Levenshtein import ratio
  LEVENSHTEIN_AVAILABLE = True
except ImportError:
  LEVENSHTEIN_AVAILABLE = False
  # Fallback: simple character-based similarity
  def ratio(s1: str, s2: str):
    """Simple similarity ratio fallback"""
    if not s1 or not s2:
      return 0.0
    s1_set = set(s1.lower())
    s2_set = set(s2.lower())
    if not s1_set:
      return 0.0
    return len(s1_set & s2_set) / len(s1_set)

router = APIRouter()

# Get API base URL from environment or use default
API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8001")

def _transform_image_url(offer: dict):
  """
  Transform image_url to use local IGDB image if available, otherwise keep original affiliate image
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

@router.get("/genres")
async def get_genres():
  """
  Get all available genres
  """
  try:
    genres = db_get_genres()
    return [g.model_dump() for g in genres]
  except Exception as e:
    raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")

@router.get("/offers_list")
async def offers_list_api(
  distributor: Optional[List[str]] = Query(None),
  genre: Optional[List[int]] = Query(None),
  sort_by: Optional[str] = Query(None),
  limit: int = Query(60, ge=1, le=200),
  offset: int = Query(0, ge=0),
):
  try:
    results = get_offers(
      distributor_names=distributor,
      genre_ids=genre,
      sort_by=sort_by,
      limit=limit,
      offset=offset
    )

    if not results:
      return []

    for offer in results:
      _transform_image_url(offer)

    return results

  except Exception as e:
    print(f"Database Error: {e}")
    raise HTTPException(status_code=500, detail="Internal Server Error")

@router.get("/topsellers")
async def get_topsellers(
  genre: Optional[List[int]] = Query(None),
  limit: int = Query(60, ge=1, le=200),
  offset: int = Query(0, ge=0),
):
  try:
    results = db_get_topsellers(
      genre_ids=genre,
      limit=limit,
      offset=offset
    )
    for offer in results:
      _transform_image_url(offer)
    return results

  except Exception as e:
    print(f"Top Sellers Query Error: {e}")
    raise HTTPException(status_code=500, detail="Error fetching top sellers")

@router.get("/search")
async def search_offers(
  q: str = Query("", description="Search query string"),
  sort_by: Optional[str] = Query(None),
  limit: int = Query(10, ge=1, le=200),
  offset: int = Query(0, ge=0)
):
  if not q or not q.strip():
    return []

  try:
    results = db_search_offers(
      q=q,
      sort_by=sort_by,
      limit=limit,
      offset=offset
    )
    for offer in results:
      _transform_image_url(offer)
    return results

  except Exception as e:
    print(f"Search unexpected error: {e}")
    raise HTTPException(status_code=500, detail="Search failed")


