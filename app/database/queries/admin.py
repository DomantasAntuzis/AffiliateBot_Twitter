from sqlmodel import select
from database.models import Offer, Item, Distributor
from database.db_session import get_session

def update_offer(offer_id: int, update_data: dict):
	with get_session() as session:
		statement = select(Offer).where(Offer.id == offer_id)
		db_offer = session.exec(statement).first()
		if db_offer is None:
			return None
        
		# 2. Dynamic Field Building (No strings needed!)
		for key, value in update_data.items():
			if hasattr(db_offer, key):
					setattr(db_offer, key, value)
			
		# 3. Save
		session.add(db_offer)
		session.commit()
		session.refresh(db_offer)
		return db_offer

#this need to be rethinked, maybe we should use a different approach to soft delete offers

def soft_delete_offer(offer_id: int):
	with get_session() as session:
		statement = select(Offer).where(Offer.id == offer_id)
		db_offer = session.exec(statement).first()
		if db_offer is None:
			return None

		db_offer.is_hidden = True
		session.add(db_offer)
		session.commit()
		session.refresh(db_offer)
		return db_offer
