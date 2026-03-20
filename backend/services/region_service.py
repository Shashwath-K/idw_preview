from backend.services.query_utils import build_dimension_filters, fetch_all, fetch_one


def get_region_kpis(
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
            COALESCE(SUM(COALESCE(e.students_total, 0)), 0) AS total_students_reached,
            COUNT(DISTINCT l.state) AS total_states,
            COALESCE(SUM(f.session_count), 0) AS total_sessions,
            COALESCE(AVG(COALESCE(e.students_total, 0)), 0) AS avg_students_per_state_period
        FROM fact_session_event f
        LEFT JOIN fact_exposure e ON e.session_key = f.session_key
        LEFT JOIN dim_date d ON d.date_key = f.date_key
        LEFT JOIN dim_location l ON l.location_key = COALESCE(f.location_key, e.location_key)
        LEFT JOIN dim_program p ON p.program_key = f.program_key
        {where_clause}
        """,
        params,
    )
    return {
        "total_students_reached": int(row.get("total_students_reached", 0) or 0),
        "total_states": int(row.get("total_states", 0) or 0),
        "total_programs": int(row.get("total_sessions", 0) or 0),
        "avg_students_per_state_period": round(float(row.get("avg_students_per_state_period", 0) or 0), 2),
    }


def get_region_impact(
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
            COALESCE(SUM(COALESCE(e.students_total, 0)), 0) AS value
        FROM fact_session_event f
        LEFT JOIN fact_exposure e ON e.session_key = f.session_key
        LEFT JOIN dim_date d ON d.date_key = f.date_key
        LEFT JOIN dim_location l ON l.location_key = COALESCE(f.location_key, e.location_key)
        LEFT JOIN dim_program p ON p.program_key = f.program_key
        {where_clause}
        GROUP BY COALESCE(l.state, 'Unknown')
        ORDER BY value DESC, label
        LIMIT 15
        """,
        params,
    )
    return [{"label": row["label"], "value": float(row["value"])} for row in rows]


def get_monthly_region_impact(
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
            COALESCE(SUM(COALESCE(e.students_total, 0)), 0) AS value,
            DATE_TRUNC('month', d.date) AS sort_key
        FROM fact_session_event f
        LEFT JOIN fact_exposure e ON e.session_key = f.session_key
        LEFT JOIN dim_date d ON d.date_key = f.date_key
        LEFT JOIN dim_location l ON l.location_key = COALESCE(f.location_key, e.location_key)
        LEFT JOIN dim_program p ON p.program_key = f.program_key
        {where_clause}
        GROUP BY DATE_TRUNC('month', d.date)
        ORDER BY DATE_TRUNC('month', d.date)
        """,
        params,
    )
    return [{"label": row["label"], "value": float(row["value"])} for row in rows]


def get_region_options() -> list[str]:
    rows = fetch_all(
        """
        SELECT DISTINCT state
        FROM dim_location
        WHERE state IS NOT NULL
        ORDER BY state
        """
    )
    return [str(row["state"]) for row in rows if row.get("state")]
