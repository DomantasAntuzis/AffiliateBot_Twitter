from sqlmodel import create_engine, Session, SQLModel
import config

# mysql+pymysql is the standard driver for Python 3
DATABASE_URL = f"mysql+pymysql://{config.DB_USER}:{config.DB_PASSWORD}@{config.DB_HOST}/{config.DB_NAME}"

# 2. The Engine (The core connection pool)
engine = create_engine(DATABASE_URL, echo=False)

# 3. This replaces 'get_connection'
def get_session():
	"""Returns a new database session."""
	return Session(engine)