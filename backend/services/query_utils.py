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


# -------------------------------------------
# V2: Multi-select & enhanced filter helpers
# -------------------------------------------

def parse_multi_param(param: str | None) -> list[str]:
    """Split a comma-separated filter string into a list of non-empty values."""
    if not param:
        return []
    return [v.strip() for v in param.split(',') if v.strip()]


def build_multi_value_clause(column: str, values: list[str], params: list) -> str | None:
    """
    Generate a SQL 'column IN (%s, %s, ...)' clause and append values to params.
    Returns the clause string, or None if values is empty.
    """
    if not values:
        return None
    placeholders = ', '.join(['%s'] * len(values))
    params.extend(values)
    return f"{column} IN ({placeholders})"


def build_quarter_clause(quarter_csv: str | None, date_col: str, params: list) -> str | None:
    """
    Generate a quarter filter clause.  quarter_csv is like "1,2" for Q1 and Q2.
    Uses the quarter column from dim_date: d.quarter IN (%s, %s).
    """
    quarters = parse_multi_param(quarter_csv)
    if not quarters:
        return None
    int_quarters = []
    for q in quarters:
        try:
            int_quarters.append(int(q))
        except ValueError:
            continue
    if not int_quarters:
        return None
    placeholders = ', '.join(['%s'] * len(int_quarters))
    params.extend(int_quarters)
    return f"{date_col} IN ({placeholders})"
