from __future__ import annotations

import difflib
import io
import json
import re
from collections.abc import Iterable
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from fastapi import APIRouter, File, Form, HTTPException, Request, UploadFile, status
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from psycopg2 import sql
from psycopg2.extras import execute_values

from backend.config import MANAGED_SOURCE_TABLES, SOURCE_DB_NAME, TEMPLATES_DIR
from backend.db import get_source_conn
from backend.elt_runner import run_elt
from backend.models.schemas import ApiMessage


router = APIRouter(tags=["upload"])
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
IGNORE_SHEET_VALUE = "__ignore__"


def _render_page(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        "upload.html",
        {
            "request": request,
            "page_title": "Upload Excel",
            "page_id": "upload",
            "managed_tables": MANAGED_SOURCE_TABLES,
        },
    )


@router.get("/upload", response_class=HTMLResponse)
def upload_page(request: Request):
    return _render_page(request)


@router.post("/upload", response_model=ApiMessage)
async def upload_workbook(file: UploadFile = File(...), mapping_json: str | None = Form(default=None)):
    filename = file.filename or ""
    if not filename.lower().endswith((".xlsx", ".xls")):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Please upload an Excel file with .xlsx or .xls extension.",
        )

    file_bytes = await file.read()
    if not file_bytes:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="The uploaded file is empty.")

    workbook = _read_workbook(file_bytes)
    user_mapping = _parse_mapping_json(mapping_json)

    with get_source_conn() as conn:
        available_tables = _fetch_available_tables(conn)
        missing_tables = [table_name for table_name in MANAGED_SOURCE_TABLES if table_name not in available_tables]
        if missing_tables:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=(
                    f"Configured source database '{SOURCE_DB_NAME}' is missing managed tables: "
                    f"{', '.join(missing_tables)}"
                ),
            )

        table_metadata = _fetch_table_metadata(conn, MANAGED_SOURCE_TABLES)
        import_plan = _build_import_plan(workbook, table_metadata, user_mapping)
        if import_plan["requires_configuration"]:
            return JSONResponse(
                status_code=status.HTTP_409_CONFLICT,
                content={
                    "message": "Manual sheet or field mapping is required before the import can continue.",
                    "details": {
                        "requires_configuration": True,
                        "filename": filename,
                        "source_database": SOURCE_DB_NAME,
                        "available_tables": list(MANAGED_SOURCE_TABLES),
                        "table_catalog": import_plan["table_catalog"],
                        "sheets": import_plan["sheet_configs"],
                    },
                },
            )

        inserted_rows: dict[str, int] = {}

        try:
            with conn.cursor() as cur:
                _truncate_source_tables(cur, MANAGED_SOURCE_TABLES)

                for table_name in MANAGED_SOURCE_TABLES:
                    table_plan = import_plan["table_plans"].get(table_name)
                    if table_plan is None:
                        inserted_rows[table_name] = 0
                        continue

                    inserted_rows[table_name] = _insert_dataframe(
                        cur=cur,
                        table_name=table_name,
                        dataframe=workbook[table_plan["sheet_name"]],
                        table_metadata=table_metadata[table_name],
                        column_map=table_plan["column_map"],
                    )
        except HTTPException:
            raise
        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Source load failed: {exc}",
            ) from exc

    try:
        executed_scripts = run_elt()
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Source load completed but ELT failed: {exc}",
        ) from exc

    return {
        "message": "Excel upload completed and ELT refresh finished successfully.",
        "details": {
            "filename": filename,
            "source_database": SOURCE_DB_NAME,
            "inserted_rows": inserted_rows,
            "executed_scripts": executed_scripts,
        },
    }


def _parse_mapping_json(mapping_json: str | None) -> dict[str, Any]:
    if not mapping_json:
        return {}

    try:
        payload = json.loads(mapping_json)
    except json.JSONDecodeError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The submitted mapping configuration is not valid JSON.",
        ) from exc

    if not isinstance(payload, dict):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The submitted mapping configuration must be a JSON object.",
        )

    return payload


def _read_workbook(file_bytes: bytes) -> dict[str, Any]:
    try:
        import pandas as pd
    except ImportError as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Pandas and openpyxl are required for Excel uploads. Install requirements before using /upload.",
        ) from exc

    try:
        workbook = pd.read_excel(io.BytesIO(file_bytes), sheet_name=None, dtype=object)
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Unable to read Excel file: {exc}") from exc

    return workbook


def _fetch_available_tables(conn) -> set[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_name = ANY(%s)
            """,
            [list(MANAGED_SOURCE_TABLES)],
        )
        return {row["table_name"] for row in cur.fetchall()}


def _fetch_table_metadata(conn, table_names: Iterable[str]) -> dict[str, list[dict[str, Any]]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT table_name, column_name, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = ANY(%s)
            ORDER BY table_name, ordinal_position
            """,
            [list(table_names)],
        )
        metadata: dict[str, list[dict[str, Any]]] = {table_name: [] for table_name in table_names}
        for row in cur.fetchall():
            metadata[row["table_name"]].append(
                {
                    "column_name": row["column_name"],
                    "required": row["is_nullable"] == "NO" and row["column_default"] is None,
                }
            )
        return metadata


def _build_import_plan(
    workbook: dict[str, Any],
    table_metadata: dict[str, list[dict[str, Any]]],
    user_mapping: dict[str, Any],
) -> dict[str, Any]:
    sheet_targets = user_mapping.get("sheet_targets", {}) if isinstance(user_mapping, dict) else {}
    column_mappings = user_mapping.get("column_mappings", {}) if isinstance(user_mapping, dict) else {}

    table_catalog = {
        table_name: [
            {"target_column": column["column_name"], "required": column["required"]}
            for column in columns
        ]
        for table_name, columns in table_metadata.items()
    }

    requires_configuration = False
    assigned_targets: dict[str, str] = {}
    table_plans: dict[str, dict[str, Any]] = {}
    sheet_configs: list[dict[str, Any]] = []

    for sheet_name, dataframe in workbook.items():
        source_columns = [str(name).strip() for name in dataframe.columns if name is not None and str(name).strip()]
        exact_match = sheet_name in table_metadata
        suggested_target = None if exact_match else _suggest_target_table(sheet_name)
        configured_target = sheet_targets.get(sheet_name)
        selected_target = configured_target if configured_target is not None else (sheet_name if exact_match else suggested_target)
        ignored = selected_target == IGNORE_SHEET_VALUE
        sheet_issues: list[str] = []
        column_configs: list[dict[str, Any]] = []
        unmatched_source_columns = list(source_columns)
        resolved_target = None if ignored else selected_target

        if not exact_match and configured_target is None:
            requires_configuration = True
            sheet_issues.append(
                "This sheet name does not match a configured source table. Choose a target table or ignore it."
            )

        if resolved_target and resolved_target not in table_metadata:
            requires_configuration = True
            sheet_issues.append(f"The selected target table '{resolved_target}' is not valid.")
            resolved_target = None

        duplicate_target_sheet = None
        if resolved_target and resolved_target in assigned_targets and assigned_targets[resolved_target] != sheet_name:
            duplicate_target_sheet = assigned_targets[resolved_target]
            requires_configuration = True
            sheet_issues.append(
                f"Target table '{resolved_target}' is already mapped from sheet '{duplicate_target_sheet}'."
            )
        elif resolved_target:
            assigned_targets[resolved_target] = sheet_name

        if resolved_target:
            source_lookup = {_normalize_identifier(column): column for column in source_columns}
            configured_columns = column_mappings.get(sheet_name, {}) if isinstance(column_mappings, dict) else {}
            selected_column_map: dict[str, str] = {}
            used_source_columns: set[str] = set()

            for column_meta in table_metadata[resolved_target]:
                target_column = column_meta["column_name"]
                auto_source = source_lookup.get(_normalize_identifier(target_column))
                chosen_source = configured_columns.get(target_column)

                if chosen_source == "":
                    chosen_source = None
                elif chosen_source is None:
                    chosen_source = auto_source
                elif chosen_source not in source_columns:
                    chosen_source = None
                    requires_configuration = True
                    sheet_issues.append(
                        f"Column mapping for '{target_column}' points to a source column that does not exist anymore."
                    )

                if chosen_source:
                    used_source_columns.add(chosen_source)
                    selected_column_map[target_column] = chosen_source
                elif column_meta["required"]:
                    requires_configuration = True
                    sheet_issues.append(f"Required target column '{target_column}' needs a source column mapping.")

                column_configs.append(
                    {
                        "target_column": target_column,
                        "required": column_meta["required"],
                        "selected_source": chosen_source,
                        "auto_source": auto_source,
                    }
                )

            unmatched_source_columns = [column for column in source_columns if column not in used_source_columns]
            if not sheet_issues and resolved_target:
                table_plans[resolved_target] = {
                    "sheet_name": sheet_name,
                    "column_map": selected_column_map,
                }

        sheet_configs.append(
            {
                "sheet_name": sheet_name,
                "source_columns": source_columns,
                "selected_target": selected_target or "",
                "suggested_target": suggested_target or "",
                "exact_match": exact_match,
                "ignored": ignored,
                "issues": list(dict.fromkeys(sheet_issues)),
                "column_mappings": column_configs,
                "unmatched_source_columns": unmatched_source_columns,
            }
        )

    return {
        "requires_configuration": requires_configuration,
        "table_catalog": table_catalog,
        "sheet_configs": sheet_configs,
        "table_plans": table_plans,
    }


def _truncate_source_tables(cur, table_names: Iterable[str]) -> None:
    identifiers = [sql.Identifier("public", table_name) for table_name in table_names]
    statement = sql.SQL("TRUNCATE TABLE {} RESTART IDENTITY CASCADE").format(sql.SQL(", ").join(identifiers))
    cur.execute(statement)


def _insert_dataframe(
    cur,
    table_name: str,
    dataframe: Any,
    table_metadata: list[dict[str, Any]],
    column_map: dict[str, str],
) -> int:
    trimmed = dataframe.dropna(how="all")
    if trimmed.empty:
        return 0

    selected_columns = [
        column_meta["column_name"]
        for column_meta in table_metadata
        if column_meta["column_name"] in column_map and column_map[column_meta["column_name"]] in trimmed.columns
    ]
    if not selected_columns:
        return 0

    records = []
    for row in trimmed.to_dict(orient="records"):
        records.append(
            tuple(_normalize_cell_value(row.get(column_map[target_column])) for target_column in selected_columns)
        )

    if not records:
        return 0

    insert_sql = sql.SQL("INSERT INTO {} ({}) VALUES %s").format(
        sql.Identifier("public", table_name),
        sql.SQL(", ").join(sql.Identifier(column_name) for column_name in selected_columns),
    )
    execute_values(cur, insert_sql.as_string(cur.connection), records, page_size=500)
    return len(records)


def _suggest_target_table(sheet_name: str) -> str | None:
    normalized_sheet_name = _normalize_identifier(sheet_name)
    normalized_targets = {_normalize_identifier(table_name): table_name for table_name in MANAGED_SOURCE_TABLES}

    if normalized_sheet_name in normalized_targets:
        return normalized_targets[normalized_sheet_name]

    close_matches = difflib.get_close_matches(normalized_sheet_name, normalized_targets.keys(), n=1, cutoff=0.6)
    if close_matches:
        return normalized_targets[close_matches[0]]

    return None


def _normalize_identifier(value: Any) -> str:
    text = str(value).strip().lower()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    return text.strip("_")


def _normalize_cell_value(value: Any) -> Any:
    if value is None:
        return None

    if isinstance(value, str):
        cleaned = value.strip()
        return cleaned or None

    if isinstance(value, Decimal):
        return value

    if isinstance(value, (datetime, date)):
        return value

    if hasattr(value, "to_pydatetime"):
        return value.to_pydatetime()

    if hasattr(value, "item") and not isinstance(value, (bytes, bytearray)):
        try:
            return value.item()
        except Exception:
            return value

    return value
