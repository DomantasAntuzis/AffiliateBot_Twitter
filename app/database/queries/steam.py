from database.db_session import get_session
from database.models import TopSeller
from sqlmodel import select
from pydantic import ValidationError
from utils.logger import logger

def get_steam_topsellers():
	"""Return list of [title, price] ordered by ranking (id)."""
	with get_session() as session:
		statement = select(TopSeller).order_by(TopSeller.id.asc())
		topsellers = session.exec(statement).all()
	return [[t.title, t.price] for t in topsellers]

def insert_topsellers(topsellers: list[tuple[int, str, str, float, int | None]]) -> tuple[int, int]:
	"""
	Replace or insert all topsellers with new data.
	topsellers: list of (id/ranking, title, norm_title, price, item_id) tuples.
	Returns tuple of (inserted, skipped) counts.
	"""
	if not topsellers:
		return 0, 0
	
	inserted = 0
	skipped = 0
	valid_topsellers = []
	
	# Validate all entries first
	for r, t, norm_t, p, item_id in topsellers:
		try:
			TopSeller(id=r, title=t, norm_title=norm_t, price=p, item_id=item_id)
			valid_topsellers.append((r, t, norm_t, p, item_id))
			inserted += 1
		except (ValueError, ValidationError, TypeError) as e:
			skipped += 1
			logger.warning(f"Skipped topseller (rank={r}, title={t}): {str(e)}")
			continue
	
	if not valid_topsellers:
		logger.warning("No valid topsellers to insert, keeping existing data")
		return 0, skipped
	
	with get_session() as session:
		for t in session.exec(select(TopSeller)).all():
			session.delete(t)
		session.add_all([
			TopSeller(id=r, title=t, norm_title=norm_t, price=p, item_id=item_id)
			for r, t, norm_t, p, item_id in valid_topsellers
		])
		session.commit()
	
	if skipped > 0:
		logger.info(f"Batch inserted {inserted} topsellers, skipped {skipped} with validation errors")
	return inserted, skipped