"""
Manual database migration script.
"""

from __future__ import annotations

import os
import re
import sys

from sqlmodel import Session, SQLModel, select, create_engine
from sqlalchemy import text

APP_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

if APP_DIR not in sys.path:
    sys.path.insert(0, APP_DIR)

import config  # noqa: E402
from database.models import Distributor, Genre  # noqa: E402


def build_server_url() -> str:
    db_port = getattr(config, "DB_PORT", "3306")
    return f"mysql+pymysql://{config.DB_USER}:{config.DB_PASSWORD}@{config.DB_HOST}:{db_port}/"


def build_database_url() -> str:
    db_port = getattr(config, "DB_PORT", "3306")
    return (
        f"mysql+pymysql://{config.DB_USER}:{config.DB_PASSWORD}"
        f"@{config.DB_HOST}:{db_port}/{config.DB_NAME}"
    )


def ensure_database_exists() -> None:
    db_name = str(config.DB_NAME).replace("`", "")
    engine = create_engine(build_server_url(), isolation_level="AUTOCOMMIT", echo=False)

    with engine.connect() as connection:
        connection.execute(
            text(
                f"CREATE DATABASE IF NOT EXISTS `{db_name}` "
                "CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
            )
        )

    print(f"Database ensured: {db_name}")


def seed_distributors(session: Session) -> None:
    distributors = [
        ("GamersGate", "https://www.gamersgate.com"),
        ("GOG", "https://www.gog.com"),
        ("IndieGala", "https://www.indiegala.com"),
        ("YUPLAY", "https://www.yuplay.com"),
    ]

    existing_names = {row[0] for row in session.exec(select(Distributor.name)).all()}

    for name, url in distributors:
        if name not in existing_names:
            session.add(Distributor(name=name, url=url))

    existing_names = {row[0] for row in session.exec(select(Genre.name)).all()}


def ensure_topsellers_schema(engine) -> None:
    """Ensure topsellers table has item_id foreign key (for existing deployments)"""
    with engine.connect() as connection:
        # Check if item_id column exists in topsellers table
        check_column = text(
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_NAME = 'topsellers' AND COLUMN_NAME = 'item_id' "
            "AND TABLE_SCHEMA = :db_name"
        )
        result = connection.execute(check_column, {"db_name": config.DB_NAME})
        column_exists = result.fetchone() is not None
        
        if not column_exists:
            print("Adding item_id column to topsellers table...")
            # Add the column with foreign key constraint
            alter_table = text(
                "ALTER TABLE topsellers "
                "ADD COLUMN item_id INT DEFAULT NULL AFTER id, "
                "ADD FOREIGN KEY (item_id) REFERENCES items(id) ON DELETE CASCADE"
            )
            connection.execute(alter_table)
            
            # Populate item_id by matching titles with items table
            populate_fk = text(
                "UPDATE topsellers ts "
                "INNER JOIN items i ON ts.title = i.title "
                "SET ts.item_id = i.id"
            )
            connection.execute(populate_fk)
            connection.commit()
            print("item_id column added and populated successfully")
        else:
            print("item_id column already exists in topsellers table")


def normalize_title_for_match(title: str | None) -> str:
    if not title:
        return ""

    normalized = title.lower().strip()
    normalized = re.sub(r"[-:;–—]", " ", normalized)
    normalized = re.sub(r"[^\w\s']", "", normalized)
    normalized = normalized.replace("'", "")

    roman_map = {
        "x": "10",
        "ix": "9",
        "viii": "8",
        "vii": "7",
        "vi": "6",
        "v": "5",
        "iv": "4",
        "iii": "3",
        "ii": "2",
        "i": "1",
    }
    for roman, arabic in roman_map.items():
        normalized = re.sub(rf"\b{roman}\b", arabic, normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized[:255]


def ensure_norm_title_schema(engine) -> None:
    """Ensure norm_title columns and indexes exist and are backfilled."""
    with engine.connect() as connection:
        # Add missing columns
        for table_name in ("items", "topsellers"):
            check_column = text(
                "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
                "WHERE TABLE_NAME = :table_name AND COLUMN_NAME = 'norm_title' "
                "AND TABLE_SCHEMA = :db_name"
            )
            exists = connection.execute(
                check_column, {"table_name": table_name, "db_name": config.DB_NAME}
            ).fetchone()
            if not exists:
                connection.execute(
                    text(f"ALTER TABLE {table_name} ADD COLUMN norm_title VARCHAR(255) DEFAULT NULL")
                )
                print(f"Added norm_title column to {table_name}")

        # Add indexes if missing
        for table_name, index_name in (
            ("items", "idx_items_norm_title"),
            ("topsellers", "idx_topsellers_norm_title"),
        ):
            check_index = text(
                "SELECT INDEX_NAME FROM INFORMATION_SCHEMA.STATISTICS "
                "WHERE TABLE_SCHEMA = :db_name AND TABLE_NAME = :table_name "
                "AND INDEX_NAME = :index_name"
            )
            index_exists = connection.execute(
                check_index,
                {
                    "db_name": config.DB_NAME,
                    "table_name": table_name,
                    "index_name": index_name,
                },
            ).fetchone()
            if not index_exists:
                connection.execute(text(f"CREATE INDEX {index_name} ON {table_name}(norm_title)"))
                print(f"Created index {index_name}")

        # Backfill items.norm_title
        item_rows = connection.execute(
            text("SELECT id, title FROM items WHERE norm_title IS NULL OR norm_title = ''")
        ).all()
        for row in item_rows:
            connection.execute(
                text("UPDATE items SET norm_title = :norm_title WHERE id = :id"),
                {"id": row[0], "norm_title": normalize_title_for_match(row[1])},
            )

        # Backfill topsellers.norm_title
        topseller_rows = connection.execute(
            text("SELECT id, title FROM topsellers WHERE norm_title IS NULL OR norm_title = ''")
        ).all()
        for row in topseller_rows:
            connection.execute(
                text("UPDATE topsellers SET norm_title = :norm_title WHERE id = :id"),
                {"id": row[0], "norm_title": normalize_title_for_match(row[1])},
            )

        # Link unmatched topsellers via exact normalized title
        connection.execute(
            text(
                "UPDATE topsellers ts "
                "JOIN items i ON ts.norm_title = i.norm_title "
                "SET ts.item_id = i.id "
                "WHERE ts.item_id IS NULL"
            )
        )

        connection.commit()
        print("norm_title schema ensured and backfilled")

def main() -> int:
    ensure_database_exists()

    database_url = build_database_url()
    engine = create_engine(database_url, echo=False)

    print("Creating schema if it does not exist...")
    SQLModel.metadata.create_all(engine)
    
    print("Ensuring topsellers schema is up to date...")
    ensure_topsellers_schema(engine)

    print("Ensuring normalized title schema is up to date...")
    ensure_norm_title_schema(engine)

    with Session(engine) as session:
        print("Seeding distributors and genres...")
        seed_distributors(session)
        session.commit()

    print("Migration completed successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())