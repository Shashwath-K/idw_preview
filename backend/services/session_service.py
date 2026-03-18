from backend.services.query_utils import build_dimension_filters, fetch_all, fetch_one


def get_session_count(start: int | None = None, end: int | None = None) -> int:
    where_clause, params = build_dimension_filters(
        start=start,
        end=end,
        region=None,
        program=None,
        year_expression="d.year",
    )
    row = fetch_one(
        f"""
        SELECT COALESCE(SUM(f.session_count), COUNT(*), 0) AS count
        FROM fact_session_event f
        LEFT JOIN dim_date d ON d.date_key = f.date_key
        {where_clause}
        """,
        params,
    )
    return int(row.get("count", 0) or 0)


def get_session_kpis(
    start: int | None = None,
    end: int | None = None,
    region: str | None = None,
    program: str | None = None,
) -> dict[str, int]:
    where_clause, params = build_dimension_filters(
        start=start,
        end=end,
        region=region,
        program=program,
        year_expression="d.year",
        location_expression="l.state",
        program_expression="p.program_name",
    )
    row = fetch_one(
        f"""
        SELECT
            COALESCE(SUM(f.session_count), COUNT(*), 0) AS total_sessions,
            COUNT(DISTINCT f.instructor_key) AS total_instructors,
            COUNT(DISTINCT l.state) AS active_regions,
            COUNT(DISTINCT p.program_name) AS total_programs
        FROM fact_session_event f
        LEFT JOIN dim_date d ON d.date_key = f.date_key
        LEFT JOIN dim_location l ON l.location_key = f.location_key
        LEFT JOIN dim_program p ON p.program_key = f.program_key
        {where_clause}
        """,
        params,
    )
    return {
        "total_sessions": int(row.get("total_sessions", 0) or 0),
        "total_instructors": int(row.get("total_instructors", 0) or 0),
        "active_regions": int(row.get("active_regions", 0) or 0),
        "total_programs": int(row.get("total_programs", 0) or 0),
    }


def get_monthly_sessions(
    start: int | None = None,
    end: int | None = None,
    region: str | None = None,
    program: str | None = None,
) -> list[dict]:
    where_clause, params = build_dimension_filters(
        start=start,
        end=end,
        region=region,
        program=program,
        year_expression="d.year",
        location_expression="l.state",
        program_expression="p.program_name",
    )
    rows = fetch_all(
        f"""
        SELECT
            TO_CHAR(DATE_TRUNC('month', d.date), 'YYYY-MM') AS label,
            COALESCE(SUM(f.session_count), COUNT(*), 0) AS value
        FROM fact_session_event f
        LEFT JOIN dim_date d ON d.date_key = f.date_key
        LEFT JOIN dim_location l ON l.location_key = f.location_key
        LEFT JOIN dim_program p ON p.program_key = f.program_key
        {where_clause}
        GROUP BY DATE_TRUNC('month', d.date)
        ORDER BY DATE_TRUNC('month', d.date)
        """,
        params,
    )
    return [{"label": row["label"], "value": float(row["value"])} for row in rows]


def get_sessions_by_region(
    start: int | None = None,
    end: int | None = None,
    region: str | None = None,
    program: str | None = None,
) -> list[dict]:
    where_clause, params = build_dimension_filters(
        start=start,
        end=end,
        region=region,
        program=program,
        year_expression="d.year",
        location_expression="l.state",
        program_expression="p.program_name",
    )
    rows = fetch_all(
        f"""
        SELECT
            COALESCE(l.state, 'Unknown') AS label,
            COALESCE(SUM(f.session_count), COUNT(*), 0) AS value
        FROM fact_session_event f
        LEFT JOIN dim_date d ON d.date_key = f.date_key
        LEFT JOIN dim_location l ON l.location_key = f.location_key
        LEFT JOIN dim_program p ON p.program_key = f.program_key
        {where_clause}
        GROUP BY COALESCE(l.state, 'Unknown')
        ORDER BY value DESC, label
        LIMIT 12
        """,
        params,
    )
    return [{"label": row["label"], "value": float(row["value"])} for row in rows]


def get_available_years() -> list[int]:
    rows = fetch_all(
        """
        SELECT DISTINCT d.year AS year
        FROM fact_session_event f
        JOIN dim_date d ON d.date_key = f.date_key
        WHERE d.year IS NOT NULL
        ORDER BY d.year
        """
    )
    return [int(row["year"]) for row in rows if row.get("year") is not None]
