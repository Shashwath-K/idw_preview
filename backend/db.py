import psycopg2
from psycopg2.extras import RealDictCursor

from backend.config import (
    ADMIN_DB_NAME,
    DATAMART_DB_NAME,
    DB_HOST,
    DB_PASSWORD,
    DB_PORT,
    DB_USER,
    SOURCE_DB_NAME,
)


def _connect(db_name: str):
    return psycopg2.connect(
        dbname=db_name,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
        cursor_factory=RealDictCursor,
    )


def get_admin_conn():
    return _connect(ADMIN_DB_NAME)


def get_source_conn():
    return _connect(SOURCE_DB_NAME)


def get_datamart_conn():
    return _connect(DATAMART_DB_NAME)


def get_conn():
    return get_datamart_conn()
