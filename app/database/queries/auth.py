from database.db_session import get_session
from database.models import User
from sqlmodel import select

def auth_user(username: str):
	with get_session() as session:
		statement = select(User).where(User.username == username)
		user = session.exec(statement).first()
		if user is None:
			return None
		user_data = user.model_dump()
		return user_data