import psycopg2
from psycopg2.extras import RealDictCursor

from backend.config import DB_HOST, DB_NAME, DB_PASSWORD, DB_PORT, DB_USER


def get_conn():
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
        cursor_factory=RealDictCursor,
    )
