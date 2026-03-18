from __future__ import annotations

from pathlib import Path

from psycopg2 import sql

from backend.config import ADMIN_DB_NAME, DATAMART_DB_NAME, SOURCE_DB_NAME
from backend.db import get_admin_conn, get_datamart_conn, get_source_conn


BASE_DIR = Path(__file__).resolve().parent.parent
SOURCE_SCHEMA_FILE = BASE_DIR / "create_source_tables.sql"
DATAMART_SCHEMA_FILE = BASE_DIR / "create_dw_tables.sql"
ETL_MAPPING_FILE = BASE_DIR / "create_etl_mapping.sql"
SOURCE_SCHEMA_NAME = "source_data_schema"
DATAMART_SCHEMA_NAME = "dw_data_schema"


def reset_databases() -> None:
    for db_name in (SOURCE_DB_NAME, DATAMART_DB_NAME):
        _recreate_database(db_name)

    _run_sql_batch(
        get_source_conn,
        [
            f"CREATE SCHEMA IF NOT EXISTS {SOURCE_SCHEMA_NAME}",
            SOURCE_SCHEMA_FILE.read_text(encoding="utf-8"),
        ],
    )
    _run_sql_batch(
        get_datamart_conn,
        [
            f"CREATE SCHEMA IF NOT EXISTS {DATAMART_SCHEMA_NAME}",
            DATAMART_SCHEMA_FILE.read_text(encoding="utf-8"),
            ETL_MAPPING_FILE.read_text(encoding="utf-8"),
        ],
    )


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


def _run_sql_batch(connection_factory, statements: list[str]) -> None:
    with connection_factory() as conn:
        with conn.cursor() as cur:
            for statement in statements:
                cur.execute(_sanitize_sql(statement))


def _sanitize_sql(sql_text: str) -> str:
    return sql_text.replace("→", "->").replace("???", "->")


if __name__ == "__main__":
    reset_databases()
    print(
        f"Reset complete. Active databases: source={SOURCE_DB_NAME}, datamart={DATAMART_DB_NAME}, admin={ADMIN_DB_NAME}"
    )
