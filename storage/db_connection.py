"""
Database connection helpers for the Real-Time Financial Anomaly Detective.

Provides:
- engine         : SQLAlchemy engine (connection pool)
- SessionLocal   : sessionmaker factory
- get_db()       : context manager that yields a managed session
- init_db()      : runs SQL setup files (timescale + postgres) via psycopg2
"""

import logging
import os
from contextlib import contextmanager
from typing import Generator

import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

from config.settings import DATABASE_URL, DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# SQLAlchemy engine & session factory
# ---------------------------------------------------------------------------
engine = create_engine(
    DATABASE_URL,
    pool_size=5,
    max_overflow=10,
    pool_pre_ping=True,
)

SessionLocal = sessionmaker(
    bind=engine,
    autocommit=False,
    autoflush=False,
)

# ---------------------------------------------------------------------------
# Session context manager
# ---------------------------------------------------------------------------

@contextmanager
def get_db() -> Generator[Session, None, None]:
    """Yield a SQLAlchemy session; commit on success, rollback on error."""
    session: Session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


# ---------------------------------------------------------------------------
# Database initialisation (runs raw SQL files)
# ---------------------------------------------------------------------------

def _get_sql_path(filename: str) -> str:
    """Return the absolute path to a SQL file in the storage/ package."""
    here = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(here, filename)


def init_db() -> None:
    """
    Execute the two SQL setup scripts using a raw psycopg2 connection.

    Order matters:
      1. timescale_setup.sql  – creates hypertables (requires TimescaleDB extension)
      2. postgres_setup.sql   – creates regular relational tables
    """
    sql_files = [
        _get_sql_path("timescale_setup.sql"),
        _get_sql_path("postgres_setup.sql"),
    ]

    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )
    conn.autocommit = True  # DDL statements need autocommit in some contexts

    try:
        with conn.cursor() as cur:
            for sql_file in sql_files:
                logger.info("Running SQL setup file: %s", sql_file)
                with open(sql_file, "r", encoding="utf-8") as fh:
                    sql = fh.read()
                cur.execute(sql)
                logger.info("Completed: %s", sql_file)
    except Exception as exc:
        logger.error("Database initialisation failed: %s", exc)
        raise
    finally:
        conn.close()

    logger.info("Database initialisation complete.")
