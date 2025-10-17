from sqlmodel import SQLModel, Field, create_engine
from typing import Optional
import sqlite3
from pathlib import Path


class Message(SQLModel, table=True):
    """Model for storing unique gossip messages."""
    
    __tablename__ = "messages"
    
    id: Optional[int] = Field(default=None, primary_key=True)
    raw: bytes = Field(unique=True, description="Raw message bytes")


def create_database(database_path: str) -> None:
    """Create the database and tables if they don't exist."""
    Path(database_path).parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(database_path)

    # Enable WAL mode for better concurrency
    conn.execute("PRAGMA journal_mode=WAL")
    # Additional performance settings
    conn.execute("PRAGMA synchronous=NORMAL")  # Faster than FULL, still safe in WAL mode
    conn.execute("PRAGMA cache_size=10000")     # Increase cache size
    conn.execute("PRAGMA temp_store=memory")    # Use memory for temp storage

    # Create the table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            raw BLOB UNIQUE NOT NULL
        )
    """)

    conn.commit()
    conn.close()