from __future__ import annotations

from pathlib import Path
from psycopg2 import sql

from backend.config import (
    DATAMART_DB_NAME,
    DB_HOST,
    DB_PASSWORD,
    DB_PORT,
    DB_SSL_MODE,
    DB_USER,
    FDW_SOURCE_SCHEMA,
    MANAGED_SOURCE_TABLES,
    SOURCE_DB_NAME,
    SOURCE_SCHEMA_NAME,
    DATAMART_SCHEMA_NAME,
)
from backend.db import get_datamart_conn
from backend.data_ops.pipeline import ELTPipeline

FDW_SERVER_NAME = "pramana_source_server"

def run_elt(script_name: str = None) -> list[str]:
    with get_datamart_conn() as conn:
        _ensure_foreign_source_access(conn)
        
    pipeline = ELTPipeline(triggered_by="upload_or_legacy")
    result = pipeline.run()
    
    if result["status"] != "success":
        raise Exception(f"ELT Pipeline failed. Check Data Ops dashboard for run_id: {result['run_id']}")
        
    return ["elt_dim.sql", "elt_fact.sql", "elt_agg.sql"]

def _ensure_foreign_source_access(conn) -> None:
    if SOURCE_DB_NAME == DATAMART_DB_NAME:
        return

    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS postgres_fdw")
        cur.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(FDW_SOURCE_SCHEMA)))
        cur.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(FDW_SOURCE_SCHEMA)))
        cur.execute(sql.SQL("DROP SERVER IF EXISTS {} CASCADE").format(sql.Identifier(FDW_SERVER_NAME)))
        cur.execute(
            sql.SQL(
                "CREATE SERVER {} FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host %s, dbname %s, port %s, sslmode %s)"
            ).format(sql.Identifier(FDW_SERVER_NAME)),
            [DB_HOST, SOURCE_DB_NAME, str(DB_PORT), DB_SSL_MODE],
        )
        cur.execute(
            sql.SQL("CREATE USER MAPPING FOR CURRENT_USER SERVER {} OPTIONS (user %s, password %s)").format(
                sql.Identifier(FDW_SERVER_NAME)
            ),
            [DB_USER, DB_PASSWORD],
        )
        cur.execute(
            sql.SQL("IMPORT FOREIGN SCHEMA {} LIMIT TO ({}) FROM SERVER {} INTO {}").format(
                sql.Identifier(SOURCE_SCHEMA_NAME),
                sql.SQL(", ").join(sql.Identifier(t) for t in MANAGED_SOURCE_TABLES),
                sql.Identifier(FDW_SERVER_NAME),
                sql.Identifier(FDW_SOURCE_SCHEMA),
            )
        )
