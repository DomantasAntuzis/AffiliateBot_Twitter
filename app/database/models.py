from datetime import datetime
from enum import Enum
from sqlmodel import Field, SQLModel, Enum as SQLModelEnum

#ITEM TYPE ENUM
class ItemType(str, Enum):
    base_game = "base game"
    dlc = "dlc"
    bundle = "bundle"
    software = "software"

class ItemGenre(SQLModel, table=True):
    __tablename__ = "item_genres"
    item_id: int = Field(foreign_key="items.id", primary_key=True)
    genre_id: int = Field(foreign_key="genres.id", primary_key=True, index=True)

class Distributor(SQLModel, table=True):
    __tablename__ = "distributors"
    id: int | None = Field(default=None, primary_key=True)
    name: str = Field(max_length=100, index=True)
    url: str = Field(max_length=255)

class Genre(SQLModel, table=True):
    __tablename__ = "genres"
    id: int | None = Field(default=None, primary_key=True)
    name: str = Field(max_length=50, index=True)

class Item(SQLModel, table=True):
    __tablename__ = "items"
    id: int | None = Field(default=None, primary_key=True)
    title: str = Field(max_length=255)
    norm_title: str = Field(max_length=255, index=True)
    item_type: ItemType = Field(
        sa_type=SQLModelEnum(ItemType, values_callable=lambda x: [e.value for e in x], native_enum=False)
    )
    igdb_id: int = Field(index=True)
    igdb_cover_image_id: str = Field(max_length=255, index=True)

class Offer(SQLModel, table=True):
    __tablename__ = "offers"
    id: int | None = Field(default=None, primary_key=True)
    item_id: int = Field(foreign_key="items.id", index=True)
    distributor_id: int = Field(foreign_key="distributors.id", index=True)
    affiliate_url: str = Field(max_length=500)
    image_url: str = Field(max_length=500)
    list_price: float = Field(decimal_places=2)
    sale_price: float = Field(decimal_places=2)
    discount: int = Field(default=0)
    fetched_at: datetime = Field(default_factory=datetime.now)

class TwitterPost(SQLModel, table=True):
    __tablename__ = "twitter_posts"
    id: int | None = Field(default=None, primary_key=True)
    offer_id: int = Field(foreign_key="offers.id", index=True)
    posted_at: datetime = Field(default_factory=datetime.now)

class TopSeller(SQLModel, table=True):
    __tablename__ = "topsellers"
    id: int = Field(default=None, primary_key=True, index=True)
    item_id: int | None = Field(default=None,foreign_key="items.id")
    title: str = Field(max_length=255)
    norm_title: str = Field(max_length=255, index=True)
    price: float = Field(decimal_places=2)