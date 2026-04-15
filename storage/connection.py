"""
Database connection helpers.

Provides:
- engine        : SQLAlchemy engine with connection pool
- SessionLocal  : sessionmaker factory
- get_db()      : context manager yielding a managed session
- init_db()     : executes SQL schema files via psycopg2
"""

import logging
import os
from contextlib import contextmanager
from typing import Generator

import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from config.settings import DATABASE_URL, DB_HOST, DB_NAME, DB_PASSWORD, DB_PORT, DB_USER

logger = logging.getLogger(__name__)

engine = create_engine(
    DATABASE_URL,
    pool_size=5,
    max_overflow=10,
    pool_pre_ping=True,
)

SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)


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


def init_db() -> None:
    """
    Execute the SQL schema files against the configured database.

    Runs timescale_setup.sql first (hypertables) then postgres_setup.sql
    (relational tables). Safe to run multiple times thanks to IF NOT EXISTS guards.
    """
    sql_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sql")
    sql_files = [
        os.path.join(sql_dir, "timescale_setup.sql"),
        os.path.join(sql_dir, "postgres_setup.sql"),
    ]

    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
    )
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            for path in sql_files:
                logger.info("Executing %s", path)
                with open(path, encoding="utf-8") as fh:
                    cur.execute(fh.read())
                logger.info("Completed %s", path)
    except Exception as exc:
        logger.error("Database initialisation failed: %s", exc)
        raise
    finally:
        conn.close()

    logger.info("Database initialisation complete.")
