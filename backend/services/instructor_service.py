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
        program=program,
        year_expression="d.year",
        location_expression="l.state",
        program_expression="p.program_name",
    )
    row = fetch_one(
        f"""
        SELECT
            COUNT(DISTINCT i.name) AS total_instructors,
            COALESCE(SUM(f.session_count), 0) AS sessions_conducted,
            COALESCE(
                SUM(f.session_count)::numeric / NULLIF(COUNT(DISTINCT i.instructor_key), 0),
                0
            ) AS avg_sessions_per_instructor,
            COALESCE(SUM(COALESCE(e.students_total, 0)), 0) AS total_students_reached
        FROM fact_session_event f
        LEFT JOIN fact_exposure e ON e.session_key = f.session_key
        LEFT JOIN dim_date d ON d.date_key = f.date_key
        LEFT JOIN dim_location l ON l.location_key = f.location_key
        LEFT JOIN dim_program p ON p.program_key = f.program_key
        LEFT JOIN dim_instructor i ON i.instructor_key = f.instructor_key
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
        program=program,
        year_expression="d.year",
        location_expression="l.state",
        program_expression="p.program_name",
    )
    rows = fetch_all(
        f"""
        SELECT
            COALESCE(i.name, 'Unknown') AS label,
            COALESCE(SUM(f.session_count), 0) AS value
        FROM fact_session_event f
        LEFT JOIN dim_date d ON d.date_key = f.date_key
        LEFT JOIN dim_location l ON l.location_key = f.location_key
        LEFT JOIN dim_program p ON p.program_key = f.program_key
        LEFT JOIN dim_instructor i ON i.instructor_key = f.instructor_key
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
        program=program,
        year_expression="d.year",
        location_expression="l.state",
        program_expression="p.program_name",
    )
    rows = fetch_all(
        f"""
        SELECT
            TO_CHAR(DATE_TRUNC('month', d.date), 'YYYY-MM') AS label,
            COALESCE(SUM(f.session_count), 0) AS value,
            DATE_TRUNC('month', d.date) AS sort_key
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
