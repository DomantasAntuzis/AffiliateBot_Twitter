from typing import Optional, List

from database.db_session import get_session
from database.models import Genre, Item, Offer, Distributor, TopSeller, ItemGenre, ItemType
from sqlmodel import select, Session, func, or_
from utils.helpers import normalize_title, normalize_match_title
from utils.logger import logger
from Levenshtein import ratio
from pydantic import ValidationError
from datetime import datetime, timedelta
import config

def ratio(s1: str, s2: str) -> float:
    if not s1 or not s2:
        return 0.0
    s1_set = set(s1.lower())
    s2_set = set(s2.lower())
    if not s1_set:
        return 0.0
    return len(s1_set & s2_set) / len(s1_set)

def get_genres():
	with get_session() as session:
		statement = select(Genre).order_by(Genre.name.asc())
		genres = session.exec(statement).all()
		return genres

def get_items():
	with get_session() as session:
		statement = select(Item).order_by(Item.title.asc())
		items = session.exec(statement).all()
		return items

def get_offer(offer_id: int):
	with get_session() as session:
		statement = (
			select(Offer, Item.title, Distributor.name)
			.join(Item, Offer.item_id == Item.id)
			.join(Distributor, Offer.distributor_id == Distributor.id)
			.where(Offer.id == offer_id)
		)

		result = session.exec(statement).first() 

		if result is None:
			return None

		offer, item_title, distributor_name = result
		# Build a plain dict for JSON serialization
		data = offer.model_dump()
		data["item_title"] = item_title
		data["distributor_name"] = distributor_name

		return data

def get_offers(
	distributor_names: Optional[List[str]] = None,
	genre_ids: Optional[List[int]] = None,
	sort_by: Optional[str] = None,
	limit: int = 60,
	offset: int = 0
):
	"""
	Builds and executes a dynamic SQLModel query based on filters.
	"""

	with get_session() as session:
		# 1. Base selection
		statement = select(Offer, Item.title, Item.igdb_cover_image_id, Distributor.name).distinct()

		# 2. Joins (Standard Inner Joins for data integrity)
		statement = statement.join(Item, Offer.item_id == Item.id)
		statement = statement.join(Distributor, Offer.distributor_id == Distributor.id)

		# 4. Dynamic Filtering
		if genre_ids:
			statement = statement.join(ItemGenre, Offer.item_id == ItemGenre.item_id)
			statement = statement.where(ItemGenre.genre_id.in_(genre_ids))

		if distributor_names:
			statement = statement.where(Distributor.name.in_(distributor_names))

		# 5. Sorting
		if sort_by == "discount_desc":
			statement = statement.order_by(Offer.discount.desc(), Offer.id.asc())
		elif sort_by == "discount_asc":
			statement = statement.order_by(Offer.discount.asc(), Offer.id.asc())
		else:
			# Deterministic Shuffle (The 7919 Prime number hash)
			statement = statement.order_by(
				func.mod(Offer.id * 7919, 1000000), 
				Offer.discount.desc(), 
				Distributor.name
			)

		# 6. Pagination
		statement = statement.offset(offset).limit(limit)
		result = session.exec(statement).all()

		# Convert the result to a list of dictionaries
		results = []

		for offer_obj, title, cover_id, dist_name in result:
			results_json = offer_obj.model_dump()
			results_json["item_title"] = title
			results_json["igdb_cover_image_id"] = cover_id
			results_json["distributor_name"] = dist_name
			results.append(results_json)
	
		return results

def get_topsellers(
	genre_ids: list[int] | None = None,
	limit: int = 60,
	offset: int = 0
):

	with get_session() as session:
		# Base Select (Note: we include TopSeller.id to use as the rank)
		statement = (
			select(Offer, Item.title, Item.igdb_cover_image_id, Distributor.name, TopSeller.id)
			.distinct()
			.join(Item, Offer.item_id == Item.id)
			.join(Distributor, Offer.distributor_id == Distributor.id)
			.join(TopSeller, Item.id == TopSeller.item_id)
		)

		# Optional Genre Filter
		if genre_ids:
			statement = statement.join(ItemGenre, Offer.item_id == ItemGenre.item_id)
			statement = statement.where(ItemGenre.genre_id.in_(genre_ids))

		# Ordering: Rank first (ts.id), then your MOD shuffle, then distributor name
		statement = statement.order_by(
			TopSeller.id.asc(),
			func.mod(Offer.id, 10),
			Distributor.name
		)

		statement = statement.offset(offset).limit(limit)
		result = session.exec(statement).all()


		results = []
		for offer_obj, title, cover_id, dist_name, rank in result:
			data = offer_obj.model_dump()
			data["item_title"] = title
			data["igdb_cover_image_id"] = cover_id
			data["distributor_name"] = dist_name
			data["topseller_rank"] = rank
			results.append(data)

		return results

def get_offer_id(
	item_title: str,
	affiliate_url: str,
	image_url: str,
	distributor_name: str
):

	with get_session() as session:
		statement = (
			select(Offer.id)
			.join(Item, Offer.item_id == Item.id)
			.join(Distributor, Offer.distributor_id == Distributor.id)
			.where(Item.title == item_title)
			.where(Offer.affiliate_url == affiliate_url)
			.where(Offer.image_url == image_url)
			.where(Distributor.name == distributor_name)
		)
		result = session.exec(statement).first()
		return result


def search_offers(
	q: str,
	sort_by: Optional[str] = None,
	limit: int = 10,
	offset: int = 0
):
	if not q or not q.strip():
		return []

	# 1. Normalization & Pattern Prep
	normalized_query = normalize_title(q.strip())
	query_words = [w for w in normalized_query.split() if len(w) >= 2]
	if not query_words:
		return []

	with get_session() as session:
		# Build LIKE patterns for the broad SQL fetch
		patterns = [f"%{normalized_query}%"]
		patterns.extend([f"%{word}%" for word in query_words[:5]])
		
		# Prefix & N-gram patterns (Simplified for SQLModel)
		for word in query_words:
			if len(word) >= 3: patterns.append(f"%{word[:3]}%")
		
		# 2. SQL Stage: Fetch broad candidate set (up to 2000)
		# We use a custom SQL expression for the REPLACE chain to match your existing logic
		clean_title_sql = func.lower(
			func.replace(func.replace(func.replace(func.replace(
				Item.title, '-', ' '), ':', ' '), ';', ' '), '–', ' ')
		)

		statement = (
			select(Offer, Item.title, Item.igdb_cover_image_id, Distributor.name)
			.join(Item, Offer.item_id == Item.id)
			.join(Distributor, Offer.distributor_id == Distributor.id)
			.where(or_(*(clean_title_sql.like(p) for p in set(patterns))))
			.limit(2000)
		)

		candidates = session.exec(statement).all()

		# 3. Python Stage: Levenshtein Scoring
		scored_offers = []
		for offer_obj, title, cover_id, dist_name in candidates:
			norm_title = normalize_title(title)
			if norm_title.startswith(normalized_query):
				match_tier = 0
			elif normalized_query in norm_title:
				match_tier = 1
			else:
				continue
				
			# Similarity Scoring
			lev_score = ratio(normalized_query, norm_title)
			combined_score = lev_score
				
			if normalized_query in norm_title: combined_score += 0.25
			if norm_title.startswith(normalized_query): combined_score += 0.2
				
			# Word match bonus
			matched_words = sum(1 for word in query_words if word in norm_title)
			combined_score += (matched_words / len(query_words)) * 0.15
				
			combined_score = min(combined_score, 1.0)

			# Filtering Threshold
			if combined_score >= 0.4 or lev_score >= 0.6:
				data = offer_obj.model_dump()
				data.update({
					"item_title": title,
					"igdb_cover_image_id": cover_id,
					"distributor_name": dist_name,
					"match_tier": match_tier,
					"relevance": combined_score
				})
				scored_offers.append(data)

		# 4. Sorting (after loop completes)
		if sort_by == "discount_desc":
			scored_offers.sort(key=lambda x: (x['match_tier'], -x.get('discount', 0), -x['relevance']))
		elif sort_by == "discount_asc":
			scored_offers.sort(key=lambda x: (x['match_tier'], x.get('discount', 0), -x['relevance']))
		else:
			scored_offers.sort(key=lambda x: (x['match_tier'], -x['relevance']))

		# 5. Final Pagination & Cleanup
		paginated = scored_offers[offset:offset + limit]
		for item in paginated:
			item.pop('match_tier', None)
			item.pop('relevance', None)

		return paginated

def batch_insert_offers(query_values: list[tuple]) -> tuple[int, int]:
	"""
	Batch insert/upsert offers. Accepts list of tuples
	Returns (inserted, skipped) counts.
	"""
	if not query_values:
		return 0, 0

	item_ids = list({v[0] for v in query_values})
	distributor_ids = list({v[1] for v in query_values})

	inserted = 0
	skipped = 0

	with get_session() as session:
		existing = session.exec(
			select(Offer).where(
				Offer.item_id.in_(item_ids),
				Offer.distributor_id.in_(distributor_ids),
			)
		).all()
		existing_map = {(o.item_id, o.distributor_id): o for o in existing}

		for v in query_values:
			try:
				item_id, dist_id, aff_url, img_url, list_p, sale_p, discount_val = v
				key = (item_id, dist_id)
				if key in existing_map:
					offer = existing_map[key]
					#every offer that already exists in db needs to have its fetched_at dzte reset
					#for removing stale offers
					offer.fetched_at = datetime.now()
					if offer is not None:
						offer.affiliate_url = aff_url
						offer.image_url = img_url
						offer.list_price = list_p
						offer.sale_price = sale_p
						offer.discount = discount_val
					inserted += 1
				else:
					new_offer = Offer(
						item_id=item_id,
						distributor_id=dist_id,
						affiliate_url=aff_url,
						image_url=img_url,
						list_price=list_p,
						sale_price=sale_p,
						discount=discount_val
					)
					session.add(new_offer)
					existing_map[key] = new_offer  # Avoid duplicate add if same key appears again in batch
				inserted += 1
			except (ValueError, ValidationError, TypeError, KeyError) as e:
				skipped += 1
				logger.warning(f"Skipped offer (item_id={item_id if 'item_id' in locals() else '?'}, dist_id={dist_id if 'dist_id' in locals() else '?'}): {str(e)}")
		if inserted > 0:
			session.commit()
		if skipped > 0:
			logger.info(f"Batch inserted {inserted} offers, skipped {skipped} with validation errors")
	return inserted, skipped

def delete_stale_offers(older_than_days: int = config.STALE_OFFERS_OLDER_THAN_DAYS) -> int:
    stale_date = datetime.now() - timedelta(days=older_than_days)
    with get_session() as session:
        stale = session.exec(
            select(Offer).where(Offer.fetched_at < stale_date)
        ).all()
        count = len(stale)
        for offer in stale:
            session.delete(offer)
        session.commit()
    return count


def batch_lookup_item_ids(unique_titles: set,) -> dict:
	"""
	Batch lookup item_id for each title (fuzzy matching via normalization).
	Returns dict: title -> item_id
	"""
	if not unique_titles:
		return {}

	normalized_to_originals = {}
	for t in unique_titles:
		norm = normalize_title(t)
		if norm not in normalized_to_originals:
			normalized_to_originals[norm] = []
		normalized_to_originals[norm].append(t)

	with get_session() as session:
		rows = session.exec(select(Item.id, Item.title)).all()
		title_to_id = {}
		for item_id, db_title in rows:
			norm_db = normalize_title(db_title)
			if norm_db in normalized_to_originals:
				for orig in normalized_to_originals[norm_db]:
					if orig not in title_to_id:
						title_to_id[orig] = item_id
	return title_to_id


def batch_lookup_distributor_ids(normalized_to_originals: dict) -> dict:
	"""
	Batch lookup distributor_id by normalized name.
	normalized_to_originals: {normalized_name: [original_name1, ...]}
	Returns dict: original_name -> distributor_id
	"""
	if not normalized_to_originals:
		return {}

	names = list(normalized_to_originals.keys())
	with get_session() as session:
		statement = select(Distributor.id, Distributor.name).where(Distributor.name.in_(names))
		rows = session.exec(statement).all()
		result = {}
		for dist_id, name in rows:
			for orig in normalized_to_originals.get(name, []):
				result[orig] = dist_id
	return result


# =============================================================================
# IGDB data service queries (replaces vanilla SQL)
# =============================================================================

def get_genre_ids(session: Session | None = None) -> set[int]:
	"""Return set of all genre ids. For IGDB genre validation."""
	own_session = False
	if session is None:
		session = get_session()
		own_session = True
	try:
		rows = session.exec(select(Genre.id)).all()
		return set(int(r[0]) if isinstance(r, (tuple, list)) else int(r) for r in (rows or []))
	finally:
		if own_session:
			session.close()


def get_distinct_item_ids_with_genres(session: Session | None = None) -> set[int]:
	"""Return set of item_ids that already have genres assigned."""
	own_session = False
	if session is None:
		session = get_session()
		own_session = True
	try:
		rows = session.exec(select(ItemGenre.item_id).distinct()).all()
		result = set()
		for r in (rows or []):
			if r is None:
				continue
			val = r[0] if isinstance(r, (tuple, list)) else r
			if val is not None:
				result.add(int(val))
		return result
	finally:
		if own_session:
			session.close()


def batch_upsert_genres(genres: list[tuple[int, str]], session: Session | None = None) -> tuple[int, int]:
	"""Insert or update genres. genres: list of (id, name).
	   Returns (upserted, skipped) counts."""
	if not genres:
		return 0, 0
	own_session = False
	if session is None:
		session = get_session()
		own_session = True
	
	upserted = 0
	skipped = 0
	
	try:
		existing = {g.id: g for g in session.exec(select(Genre)).all()}
		for gid, name in genres:
			try:
				if gid in existing:
					existing[gid].name = name
					upserted += 1
				else:
					session.add(Genre(id=gid, name=name))
					upserted += 1
			except (ValueError, ValidationError, TypeError) as e:
				skipped += 1
				logger.warning(f"Skipped genre (id={gid}, name={name}): {str(e)}")
				continue
		if upserted > 0 and own_session:
			session.commit()
		if skipped > 0:
			logger.info(f"Batch upserted {upserted} genres, skipped {skipped} with validation errors")
		return upserted, skipped
	finally:
		if own_session:
			session.close()


def batch_upsert_items(items: list[tuple], session: Session | None = None) -> tuple[int, int]:
	"""
	Insert or update items. items: list of (title, item_type, igdb_id, igdb_cover_image_id).
	Updates igdb_cover_image_id only when new value is valid (not null/empty/'0').
	Returns (inserted, skipped) counts.
	"""
	if not items:
		return 0, 0
	own_session = False
	if session is None:
		session = get_session()
		own_session = True
	
	inserted = 0
	skipped = 0
	
	try:
		igdb_ids = [r[2] for r in items]
		existing = {i.igdb_id: i for i in session.exec(select(Item).where(Item.igdb_id.in_(igdb_ids))).all() if i.igdb_id is not None}
		for title, item_type, igdb_id, cover_id in items:
			try:
				item_type_val = ItemType(item_type) if isinstance(item_type, str) else item_type
				normalized_title = normalize_match_title(title or "")

				normalized_cover = str(cover_id).strip if cover_id else None
				if normalized_cover in ("", "0"):
					normalized_cover = none

				if igdb_id in existing:
					obj = existing[igdb_id]
					obj.title = title
					obj.norm_title = normalized_title
					if normalized_cover:
						obj.igdb_cover_image_id = normalized_cover
					inserted += 1
				else:
					session.add(
						Item(
							title=title,
							norm_title=normalized_title,
							item_type=item_type_val,
							igdb_id=igdb_id,
							igdb_cover_image_id=normalized_cover,
						)
					)
					inserted += 1
			except (ValueError, ValidationError, TypeError) as e:
				skipped += 1
				logger.warning(f"Skipped item (igdb_id={igdb_id}, title={title}): {str(e)}")
				continue
		if inserted > 0 and own_session:
			session.commit()
		if skipped > 0:
			logger.info(f"Batch upserted {inserted} items, skipped {skipped} with validation errors")
		return inserted, skipped
	finally:
		if own_session:
			session.close()


def backfill_missing_item_norm_titles(session: Session | None = None) -> int:
	"""Populate norm_title for items where it is null/empty."""
	own_session = False
	if session is None:
		session = get_session()
		own_session = True

	updated = 0
	try:
		statement = select(Item).where(or_(Item.norm_title.is_(None), Item.norm_title == ""))
		rows = session.exec(statement).all()
		for row in rows:
			row.norm_title = normalize_match_title(row.title or "")
			updated += 1

		if own_session and updated:
			session.commit()
		return updated
	finally:
		if own_session:
			session.close()


def get_items_by_igdb_ids(igdb_ids: list[int], session: Session | None = None) -> dict[int, int]:
	"""Return mapping igdb_id -> item_id (id) for given igdb_ids."""
	if not igdb_ids:
		return {}
	own_session = False
	if session is None:
		session = get_session()
		own_session = True
	try:
		statement = select(Item.id, Item.igdb_id).where(Item.igdb_id.in_(igdb_ids))
		rows = session.exec(statement).all()
		result = {}
		for item_id, igdb_id in rows:
			if igdb_id is not None:
				result[igdb_id] = item_id
		return result
	finally:
		if own_session:
			session.close()


def batch_upsert_item_genres(item_genres: list[tuple[int, int]], session: Session | None = None) -> tuple[int, int]:
	"""Insert item-genre relationships. item_genres: list of (item_id, genre_id). No-op on duplicate.
		Returns (inserted, skipped) counts.
	"""
	if not item_genres:
		return 0, 0
	own_session = False
	if session is None:
		session = get_session()
		own_session = True
	
	inserted = 0
	skipped = 0
	
	try:
		item_ids = list({i for i, _ in item_genres})
		existing = {(ig.item_id, ig.genre_id) for ig in session.exec(select(ItemGenre).where(ItemGenre.item_id.in_(item_ids))).all()}
		for item_id, genre_id in item_genres:
			try:
				if (item_id, genre_id) not in existing:
					session.add(ItemGenre(item_id=item_id, genre_id=genre_id))
					existing.add((item_id, genre_id))
					inserted += 1
			except (ValueError, ValidationError, TypeError) as e:
				skipped += 1
				logger.warning(f"Skipped genre link (item_id={item_id}, genre_id={genre_id}): {str(e)}")
				continue
		if inserted > 0 and own_session:
			session.commit()
		if skipped > 0:
			logger.info(f"Batch upserted {inserted} genre links, skipped {skipped} with validation errors")
		return inserted, skipped
	finally:
		if own_session:
			session.close()


def get_items_with_igdb_cover_for_download(session: Session | None = None) -> list[dict]:
	"""Return list of {id, igdb_cover_image_id} for items with valid cover image IDs."""
	own_session = False
	if session is None:
		session = get_session()
		own_session = True
	try:
		statement = (
			select(Item.id, Item.igdb_cover_image_id)
			.where(Item.igdb_cover_image_id.isnot(None))
			.where(Item.igdb_cover_image_id != '')
			.where(Item.igdb_cover_image_id != '0')
		)
		rows = session.exec(statement).all()
		return [{"id": r[0], "igdb_cover_image_id": r[1]} for r in rows if r[1]]
	finally:
		if own_session:
			session.close()
