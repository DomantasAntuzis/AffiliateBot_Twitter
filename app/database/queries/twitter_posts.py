from database.db_session import get_session
from database.models import TwitterPost
from sqlmodel import select
import config

def get_recent_posted_titles(limit=config.POSTED_GAMES_LIMIT):
	with get_session() as session:
		statement = select(TwitterPost).limit(limit)
		posts = session.exec(statement).all()
		return [post.model_dump() for post in posts]

def insert_twitter_post(offer_id: int):
	"""Record a tweeted offer in the database."""
	with get_session() as session:
		post = TwitterPost(offer_id=offer_id)
		session.add(post)
		session.commit()
