from backend.services.query_utils import build_dimension_filters, fetch_all, fetch_one


def _type_expression() -> str:
    return "COALESCE(NULLIF(INITCAP(TRIM(i.instructor_type)), ''), 'Unknown')"


def _region_expression() -> str:
    return "COALESCE(NULLIF(MAX(l.state), ''), NULLIF(MAX(i.region_assigned), ''), 'Unknown')"


def get_instructor_kpis(
    start: int | None = None,
    end: int | None = None,
    region: str | None = None,
    program: str | None = None,
    instructor: str | None = None,
) -> dict[str, int | float | str]:
    where_clause, params = build_dimension_filters(
        start=start,
        end=end,
        region=region,
        program=program,
        year_expression="d.year",
        location_expression="l.state",
        program_expression="p.program_name",
        instructor=instructor,
        instructor_expression="i.instructor_type",
    )
    row = fetch_one(
        f"""
        SELECT
            COUNT(DISTINCT i.instructor_key) AS total_instructors,
            COALESCE(SUM(f.session_count), 0) AS sessions_conducted,
            COALESCE(
                SUM(f.session_count)::numeric / NULLIF(COUNT(DISTINCT i.instructor_key), 0),
                0
            ) AS avg_sessions_per_instructor,
            COALESCE(
                COUNT(DISTINCT CASE WHEN COALESCE(f.is_overdue, FALSE) THEN f.session_key END),
                0
            ) AS unprocessed_sessions
        FROM fact_session_event f
        LEFT JOIN dim_date d ON d.date_key = f.date_key
        LEFT JOIN dim_location l ON l.location_key = f.location_key
        LEFT JOIN dim_program p ON p.program_key = f.program_key
        LEFT JOIN dim_instructor i ON i.instructor_key = f.instructor_key
        {where_clause}
        """,
        params,
    )
    top_region = fetch_one(
        f"""
        SELECT
            COALESCE(NULLIF(l.state, ''), NULLIF(i.region_assigned, ''), 'Unknown') AS top_region,
            COALESCE(SUM(f.session_count), 0) AS top_region_sessions
        FROM fact_session_event f
        LEFT JOIN dim_date d ON d.date_key = f.date_key
        LEFT JOIN dim_location l ON l.location_key = f.location_key
        LEFT JOIN dim_program p ON p.program_key = f.program_key
        LEFT JOIN dim_instructor i ON i.instructor_key = f.instructor_key
        {where_clause}
        GROUP BY COALESCE(NULLIF(l.state, ''), NULLIF(i.region_assigned, ''), 'Unknown')
        ORDER BY top_region_sessions DESC, top_region
        LIMIT 1
        """,
        params,
    )
    return {
        "total_instructors": int(row.get("total_instructors", 0) or 0),
        "avg_sessions_per_instructor": round(float(row.get("avg_sessions_per_instructor", 0) or 0), 1),
        "top_region": top_region.get("top_region", "-") or "-",
        "top_region_sessions": int(top_region.get("top_region_sessions", 0) or 0),
        "unprocessed_sessions": int(row.get("unprocessed_sessions", 0) or 0),
    }


def get_instructor_session_log(
    start: int | None = None,
    end: int | None = None,
    region: str | None = None,
    program: str | None = None,
    instructor: str | None = None,
    limit: int = 20,
) -> list[dict]:
    where_clause, params = build_dimension_filters(
        start=start,
        end=end,
        region=region,
        program=program,
        year_expression="d.year",
        location_expression="l.state",
        program_expression="p.program_name",
        instructor=instructor,
        instructor_expression="i.instructor_type",
    )
    rows = fetch_all(
        f"""
        SELECT
            COALESCE(NULLIF(i.name, ''), 'Unknown') AS name,
            {_type_expression()} AS instructor_type,
            {_region_expression()} AS region,
            COALESCE(SUM(f.session_count), 0) AS sessions,
            COALESCE(SUM(COALESCE(e.students_total, 0)), 0) AS students,
            TO_CHAR(MAX(d.date), 'Mon DD') AS last_session
        FROM fact_session_event f
        LEFT JOIN fact_exposure e ON e.session_key = f.session_key
        LEFT JOIN dim_date d ON d.date_key = f.date_key
        LEFT JOIN dim_location l ON l.location_key = f.location_key
        LEFT JOIN dim_program p ON p.program_key = f.program_key
        LEFT JOIN dim_instructor i ON i.instructor_key = f.instructor_key
        {where_clause}
        GROUP BY i.instructor_key, COALESCE(NULLIF(i.name, ''), 'Unknown'), {_type_expression()}
        ORDER BY sessions DESC, students DESC, name
        LIMIT %s
        """,
        [*params, limit],
    )
    return [
        {
            "name": row["name"],
            "type": row["instructor_type"],
            "region": row["region"],
            "sessions": int(row["sessions"] or 0),
            "students": int(row["students"] or 0),
            "last_session": row.get("last_session") or "-",
        }
        for row in rows
    ]


def get_multi_program_instructors(
    start: int | None = None,
    end: int | None = None,
    region: str | None = None,
    program: str | None = None,
    instructor: str | None = None,
    limit: int = 5,
) -> list[dict]:
    where_clause, params = build_dimension_filters(
        start=start,
        end=end,
        region=region,
        program=program,
        year_expression="d.year",
        location_expression="l.state",
        program_expression="p.program_name",
        instructor=instructor,
        instructor_expression="i.instructor_type",
    )
    rows = fetch_all(
        f"""
        SELECT
            COALESCE(NULLIF(i.name, ''), 'Unknown') AS name,
            {_type_expression()} AS instructor_type,
            {_region_expression()} AS region,
            COUNT(DISTINCT f.program_key) AS programs,
            COALESCE(SUM(f.session_count), 0) AS sessions
        FROM fact_session_event f
        LEFT JOIN dim_date d ON d.date_key = f.date_key
        LEFT JOIN dim_location l ON l.location_key = f.location_key
        LEFT JOIN dim_program p ON p.program_key = f.program_key
        LEFT JOIN dim_instructor i ON i.instructor_key = f.instructor_key
        {where_clause}
        GROUP BY i.instructor_key, COALESCE(NULLIF(i.name, ''), 'Unknown'), {_type_expression()}
        HAVING COUNT(DISTINCT f.program_key) > 1
        ORDER BY programs DESC, sessions DESC, name
        LIMIT %s
        """,
        [*params, limit],
    )
    return [
        {
            "name": row["name"],
            "type": row["instructor_type"],
            "region": row["region"],
            "programs": int(row["programs"] or 0),
            "sessions": int(row["sessions"] or 0),
            "initials": "".join(part[0] for part in str(row["name"]).split()[:2]).upper() or "NA",
        }
        for row in rows
    ]


def get_sessions_by_instructor_type(
    start: int | None = None,
    end: int | None = None,
    region: str | None = None,
    program: str | None = None,
    instructor: str | None = None,
) -> list[dict]:
    where_clause, params = build_dimension_filters(
        start=start,
        end=end,
        region=region,
        program=program,
        year_expression="d.year",
        location_expression="l.state",
        program_expression="p.program_name",
        instructor=instructor,
        instructor_expression="i.instructor_type",
    )
    rows = fetch_all(
        f"""
        SELECT
            {_type_expression()} AS label,
            COALESCE(SUM(f.session_count), 0) AS value
        FROM fact_session_event f
        LEFT JOIN dim_date d ON d.date_key = f.date_key
        LEFT JOIN dim_location l ON l.location_key = f.location_key
        LEFT JOIN dim_program p ON p.program_key = f.program_key
        LEFT JOIN dim_instructor i ON i.instructor_key = f.instructor_key
        {where_clause}
        GROUP BY {_type_expression()}
        ORDER BY value DESC, label
        """,
        params,
    )
    return [{"label": row["label"], "value": float(row["value"] or 0)} for row in rows]


def get_instructor_type_options() -> list[str]:
    rows = fetch_all(
        """
        SELECT DISTINCT instructor_type
        FROM dim_instructor
        WHERE instructor_type IS NOT NULL
        ORDER BY instructor_type
        """
    )
    return [str(row["instructor_type"]) for row in rows if row.get("instructor_type")]


def get_instructor_productivity(
    start: int | None = None,
    end: int | None = None,
    region: str | None = None,
    program: str | None = None,
    instructor: str | None = None,
    limit: int = 20,
) -> list[dict]:
    rows = get_instructor_session_log(start=start, end=end, region=region, program=program, instructor=instructor, limit=limit)
    return [{"label": row["name"], "value": float(row["sessions"])} for row in rows]


def get_monthly_instructor_activity(
    start: int | None = None,
    end: int | None = None,
    region: str | None = None,
    program: str | None = None,
    instructor: str | None = None,
) -> list[dict]:
    where_clause, params = build_dimension_filters(
        start=start,
        end=end,
        region=region,
        program=program,
        year_expression="d.year",
        location_expression="l.state",
        program_expression="p.program_name",
        instructor=instructor,
        instructor_expression="i.instructor_type",
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
