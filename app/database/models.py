from datetime import datetime
from enum import Enum
from sqlmodel import Field, Relationship, SQLModel, Enum as SQLModelEnum

#ITEM TYPE ENUM
class ItemType(str, Enum):
    base_game = "base game"
    dlc = "dlc"
    bundle = "bundle"
    software = "software"

#USER ROLE ENUM
class UserRole(str, Enum):
    admin = "admin"
    user = "visitor"

class ItemGenre(SQLModel, table=True):
    __tablename__ = "item_genres"
    item_id: int | None = Field(default=None, foreign_key="items.id", primary_key=True)
    genre_id: int | None = Field(default=None, foreign_key="genres.id", primary_key=True)

class Distributor(SQLModel, table=True):
    __tablename__ = "distributors"
    id: int | None = Field(default=None, primary_key=True)
    name: str = Field(max_length=100)
    url: str = Field(max_length=255)

class Genre(SQLModel, table=True):
    __tablename__ = "genres"
    id: int | None = Field(default=None, primary_key=True)
    name: str = Field(max_length=50)

class Item(SQLModel, table=True):
    __tablename__ = "items"
    id: int | None = Field(default=None, primary_key=True)
    title: str = Field(max_length=255)
    item_type: ItemType = Field(
        sa_type=SQLModelEnum(ItemType, values_callable=lambda x: [e.value for e in x], native_enum=False)
    )
    igdb_id: int | None = Field(default=None)
    igdb_cover_image_id: str | None = Field(default=None, max_length=255)

class Offer(SQLModel, table=True):
    __tablename__ = "offers"
    id: int | None = Field(default=None, primary_key=True)
    item_id: int | None = Field(foreign_key="items.id")
    distributor_id: int | None = Field(foreign_key="distributors.id")
    affiliate_url: str = Field(max_length=500)
    image_url: str | None = Field(default=None, max_length=500)
    list_price: float = Field(default=None, decimal_places=2)
    sale_price: float = Field(decimal_places=2)
    discount: int = Field()
    fetched_at: datetime = Field(default_factory=datetime.now)
    
    # Control Flags
    is_manually_edited: bool = Field(default=False)
    is_hidden: bool = Field(default=False)
    is_valid: bool = Field(default=True)

class TwitterPost(SQLModel, table=True):
    __tablename__ = "twitter_posts"
    id: int | None = Field(default=None, primary_key=True)
    offer_id: int = Field(foreign_key="offers.id")
    posted_at: datetime = Field(default_factory=datetime.now)

class User(SQLModel, table=True):
    __tablename__ = "users"
    id: int | None = Field(default=None, primary_key=True)
    username: str = Field(max_length=50, unique=True)
    password_hash: str = Field(max_length=255)
    role: UserRole
    created_at: datetime = Field(default_factory=datetime.now)

class TopSeller(SQLModel, table=True):
    __tablename__ = "topsellers"
    id: int = Field(primary_key=True)
    title: str = Field(max_length=255)
    price: float = Field(decimal_places=2)