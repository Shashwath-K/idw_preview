from backend.services.query_utils import build_dimension_filters, fetch_all, fetch_one


def get_total_students(
    start: int | None = None,
    end: int | None = None,
    region: str | None = None,
    program: str | None = None,
) -> int:
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
        SELECT COALESCE(SUM(e.students_total), 0) AS total_students
        FROM fact_exposure e
        LEFT JOIN fact_session_event f ON f.session_key = e.session_key
        LEFT JOIN dim_date d ON d.date_key = f.date_key
        LEFT JOIN dim_location l ON l.location_key = f.location_key
        LEFT JOIN dim_program p ON p.program_key = f.program_key
        {where_clause}
        """,
        params,
    )
    return int(row.get("total_students", 0) or 0)


def get_exposure_kpis(
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
            COALESCE(SUM(e.students_total), 0) AS total_students,
            COUNT(DISTINCT p.program_name) AS total_programs,
            COUNT(DISTINCT l.state) AS total_regions,
            COALESCE(AVG(e.students_total), 0) AS avg_students_per_exposure
        FROM fact_exposure e
        LEFT JOIN fact_session_event f ON f.session_key = e.session_key
        LEFT JOIN dim_date d ON d.date_key = f.date_key
        LEFT JOIN dim_location l ON l.location_key = f.location_key
        LEFT JOIN dim_program p ON p.program_key = f.program_key
        {where_clause}
        """,
        params,
    )
    return {
        "total_students": int(row.get("total_students", 0) or 0),
        "total_programs": int(row.get("total_programs", 0) or 0),
        "total_regions": int(row.get("total_regions", 0) or 0),
        "avg_students_per_exposure": round(float(row.get("avg_students_per_exposure", 0) or 0), 2),
    }


def get_program_metrics(
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
            COALESCE(p.program_name, 'Unknown') AS label,
            COALESCE(SUM(e.students_total), 0) AS value
        FROM fact_exposure e
        LEFT JOIN fact_session_event f ON f.session_key = e.session_key
        LEFT JOIN dim_date d ON d.date_key = f.date_key
        LEFT JOIN dim_location l ON l.location_key = f.location_key
        LEFT JOIN dim_program p ON p.program_key = f.program_key
        {where_clause}
        GROUP BY COALESCE(p.program_name, 'Unknown')
        ORDER BY value DESC, label
        LIMIT %s
        """,
        [*params, limit],
    )
    return [{"label": row["label"], "value": float(row["value"])} for row in rows]


def get_program_distribution(
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
            COALESCE(p.program_name, 'Unknown') AS label,
            COUNT(*) AS value
        FROM fact_exposure e
        LEFT JOIN fact_session_event f ON f.session_key = e.session_key
        LEFT JOIN dim_date d ON d.date_key = f.date_key
        LEFT JOIN dim_location l ON l.location_key = f.location_key
        LEFT JOIN dim_program p ON p.program_key = f.program_key
        {where_clause}
        GROUP BY COALESCE(p.program_name, 'Unknown')
        ORDER BY value DESC, label
        LIMIT 10
        """,
        params,
    )
    return [{"label": row["label"], "value": float(row["value"])} for row in rows]


def get_program_options() -> list[str]:
    rows = fetch_all(
        """
        SELECT DISTINCT program_name
        FROM dim_program
        WHERE program_name IS NOT NULL
        ORDER BY program_name
        """
    )
    return [str(row["program_name"]) for row in rows if row.get("program_name")]
