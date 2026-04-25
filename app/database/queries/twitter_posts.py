from database.db_session import get_session
from database.models import TwitterPost, Offer, Item
from sqlmodel import select, func
import config
from datetime import datetime

def get_recent_posted_titles(limit=config.POSTED_GAMES_LIMIT):
	with get_session() as session:
		statement = (
			select(Item.title)
			.join(Offer, Offer.item_id == Item.id)
			.join(TwitterPost, TwitterPost.offer_id == Offer.id)
			.order_by(TwitterPost.posted_at.desc())
			.limit(limit)
		)
		rows = session.exec(statement).all()
		return [title for title in rows if title]


def get_today_post_count() -> int:
	"""Return number of posts recorded today (server local date)."""
	today = datetime.now().date()
	with get_session() as session:
		statement = select(func.count(TwitterPost.id)).where(func.date(TwitterPost.posted_at) == today)
		count = session.exec(statement).one()
		return int(count or 0)

def insert_twitter_post(offer_id: int):
	"""Record a tweeted offer in the database."""
	with get_session() as session:
		post = TwitterPost(offer_id=offer_id)
		session.add(post)
		session.commit()
