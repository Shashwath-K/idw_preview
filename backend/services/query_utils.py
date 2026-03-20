from collections.abc import Sequence

from backend.db import get_datamart_conn


def build_dimension_filters(
    *,
    start: int | None,
    end: int | None,
    region: str | None,
    program: str | None,
    date_expression: str | None = None,
    year_expression: str | None = None,
    location_expression: str | None = None,
    program_expression: str | None = None,
    instructor: str | None = None,
    instructor_expression: str | None = None,
) -> tuple[str, list[object]]:
    clauses: list[str] = []
    params: list[object] = []

    if start is not None:
        if year_expression:
            clauses.append(f"{year_expression} >= %s")
            params.append(start)
        elif date_expression:
            clauses.append(f"EXTRACT(YEAR FROM {date_expression}) >= %s")
            params.append(start)

    if end is not None:
        if year_expression:
            clauses.append(f"{year_expression} <= %s")
            params.append(end)
        elif date_expression:
            clauses.append(f"EXTRACT(YEAR FROM {date_expression}) <= %s")
            params.append(end)

    if region and location_expression:
        clauses.append(f"{location_expression} = %s")
        params.append(region)

    if program and program_expression:
        clauses.append(f"{program_expression} = %s")
        params.append(program)

    if instructor and instructor_expression:
        clauses.append(f"{instructor_expression} = %s")
        params.append(instructor)

    if not clauses:
        return "", params

    return "WHERE " + " AND ".join(clauses), params


def fetch_one(query: str, params: Sequence[object] | None = None) -> dict:
    with get_datamart_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params or [])
            row = cur.fetchone()
            return dict(row or {})


def fetch_all(query: str, params: Sequence[object] | None = None) -> list[dict]:
    with get_datamart_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params or [])
            return [dict(row) for row in cur.fetchall()]
