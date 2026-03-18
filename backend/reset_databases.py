from __future__ import annotations

from pathlib import Path

from psycopg2 import sql

from backend.config import ADMIN_DB_NAME, DATAMART_DB_NAME, SOURCE_DB_NAME
from backend.db import get_admin_conn, get_datamart_conn, get_source_conn


BASE_DIR = Path(__file__).resolve().parent.parent
SOURCE_SCHEMA_FILE = BASE_DIR / "source_data_schema.sql"
DATAMART_SCHEMA_FILE = BASE_DIR / "dw_data.sql"


def reset_databases() -> None:
    for db_name in (SOURCE_DB_NAME, DATAMART_DB_NAME):
        _recreate_database(db_name)

    _run_sql_file(get_source_conn, SOURCE_SCHEMA_FILE)
    _run_sql_file(get_datamart_conn, DATAMART_SCHEMA_FILE)


def _recreate_database(db_name: str) -> None:
    conn = get_admin_conn()
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = %s AND pid <> pg_backend_pid()",
                [db_name],
            )
            cur.execute(sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(db_name)))
            cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name)))
    finally:
        conn.close()


def _run_sql_file(connection_factory, sql_path: Path) -> None:
    sql_text = sql_path.read_text(encoding="utf-8")
    with connection_factory() as conn:
        with conn.cursor() as cur:
            cur.execute(sql_text)


if __name__ == "__main__":
    reset_databases()
    print(
        f"Reset complete. Active databases: source={SOURCE_DB_NAME}, datamart={DATAMART_DB_NAME}, admin={ADMIN_DB_NAME}"
    )
