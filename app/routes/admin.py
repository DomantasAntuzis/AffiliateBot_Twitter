from typing import Optional
from database.queries.admin import (
	soft_delete_offer,
	update_offer as db_update_offer,
)
from database.queries.items import get_offer
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from routes.auth import require_admin

admin_router = APIRouter()

class OfferUpdateRequest(BaseModel):
	affiliate_url: Optional[str] = None
	image_url: Optional[str] = None
	list_price: Optional[float] = None
	sale_price: Optional[float] = None
	discount: Optional[int] = None
	is_valid: Optional[bool] = None


@admin_router.put("/offers/{offer_id}")
async def update_offer(
	offer_id: int, update_data: OfferUpdateRequest, current_user: dict = Depends(require_admin),
):
	try:
		# Build update query dynamically based on provided fields
		update_params = {}

		if update_data.affiliate_url is not None:
			update_params["affiliate_url"] = update_data.affiliate_url

		if update_data.image_url is not None:
			update_params["image_url"] = update_data.image_url

		if update_data.list_price is not None:
			update_params["list_price"] = update_data.list_price

		if update_data.sale_price is not None:
			update_params["sale_price"] = update_data.sale_price

		if update_data.discount is not None:
			update_params["discount"] = update_data.discount

		if update_data.is_valid is not None:
			update_params["is_valid"] = update_data.is_valid

		update_params["is_manually_edited"] = True

		updated_offer = db_update_offer(offer_id, update_params)

		if not updated_offer:
			raise HTTPException(status_code=404, detail="Offer not found")

		new_offer_data = get_offer(offer_id)

		return new_offer_data

	except Exception as e:
		raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")


@admin_router.delete("/offers/{offer_id}")
async def delete_offer(
	offer_id: int, current_user: dict = Depends(require_admin)
):

	try:
		deleted = soft_delete_offer(offer_id)
		if not deleted:
				raise HTTPException(status_code=404, detail="Offer not found")

		return {"message": "Offer deleted successfully", "offer_id": offer_id}

	except Exception as e:
		raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")
