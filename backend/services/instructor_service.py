from backend.services.query_utils import build_dimension_filters, fetch_all, fetch_one


def get_instructor_kpis(
    start: int | None = None,
    end: int | None = None,
    region: str | None = None,
    program: str | None = None,
) -> dict[str, float]:
    where_clause, params = build_dimension_filters(
        start=start,
        end=end,
        region=region,
        program=None,
        year_expression="(f.month_key / 100)",
        location_expression="l.state",
    )
    row = fetch_one(
        f"""
        SELECT
            COUNT(DISTINCT i.name) AS total_instructors,
            COALESCE(SUM(f.sessions_conducted), 0) AS sessions_conducted,
            COALESCE(AVG(f.sessions_conducted), 0) AS avg_sessions_per_instructor,
            COALESCE(SUM(f.students_reached), 0) AS total_students_reached
        FROM fact_instructor_productivity f
        LEFT JOIN dim_instructor i ON i.instructor_key = f.instructor_key
        LEFT JOIN dim_location l ON l.location_key = f.location_key
        {where_clause}
        """,
        params,
    )
    return {
        "total_instructors": int(row.get("total_instructors", 0) or 0),
        "sessions_conducted": int(row.get("sessions_conducted", 0) or 0),
        "avg_sessions_per_instructor": round(float(row.get("avg_sessions_per_instructor", 0) or 0), 2),
        "total_students_reached": int(row.get("total_students_reached", 0) or 0),
    }


def get_instructor_productivity(
    start: int | None = None,
    end: int | None = None,
    region: str | None = None,
    program: str | None = None,
    limit: int = 10,
) -> list[dict]:
    where_clause, params = build_dimension_filters(
        start=start,
        end=end,
        region=region,
        program=None,
        year_expression="(f.month_key / 100)",
        location_expression="l.state",
    )
    rows = fetch_all(
        f"""
        SELECT
            COALESCE(i.name, 'Unknown') AS label,
            COALESCE(SUM(f.sessions_conducted), 0) AS value
        FROM fact_instructor_productivity f
        LEFT JOIN dim_instructor i ON i.instructor_key = f.instructor_key
        LEFT JOIN dim_location l ON l.location_key = f.location_key
        {where_clause}
        GROUP BY COALESCE(i.name, 'Unknown')
        ORDER BY value DESC, label
        LIMIT %s
        """,
        [*params, limit],
    )
    return [{"label": row["label"], "value": float(row["value"])} for row in rows]


def get_monthly_instructor_activity(
    start: int | None = None,
    end: int | None = None,
    region: str | None = None,
    program: str | None = None,
) -> list[dict]:
    where_clause, params = build_dimension_filters(
        start=start,
        end=end,
        region=region,
        program=None,
        year_expression="(f.month_key / 100)",
        location_expression="l.state",
    )
    rows = fetch_all(
        f"""
        SELECT
            CONCAT(LEFT(f.month_key::text, 4), '-', RIGHT(f.month_key::text, 2)) AS label,
            COALESCE(SUM(f.sessions_conducted), 0) AS value,
            f.month_key AS sort_key
        FROM fact_instructor_productivity f
        LEFT JOIN dim_location l ON l.location_key = f.location_key
        {where_clause}
        GROUP BY f.month_key
        ORDER BY f.month_key
        """,
        params,
    )
    return [{"label": row["label"], "value": float(row["value"])} for row in rows]
