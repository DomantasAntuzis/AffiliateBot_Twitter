from database.db_session import get_session
from database.models import TopSeller
from sqlmodel import select

def get_steam_topsellers():
	"""Return list of [title, price] ordered by ranking (id)."""
	with get_session() as session:
		statement = select(TopSeller).order_by(TopSeller.id.asc())
		topsellers = session.exec(statement).all()
	return [[t.title, t.price] for t in topsellers]

def insert_topsellers(topsellers: list[tuple[int, str, float]]) -> int:
	"""
	Replace or insert all topsellers with new data.
	topsellers: list of (id/ranking, title, price) tuples.
	Returns number of rows inserted.
	"""
	if not topsellers:
		return 0
	with get_session() as session:
		for t in session.exec(select(TopSeller)).all():
			session.delete(t)
		session.add_all([TopSeller(id=r, title=t, price=p) for r, t, p in topsellers])
		session.commit()
	return len(topsellers)