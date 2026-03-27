from backend.services.query_utils import build_dimension_filters, fetch_all, fetch_one


def _base_where(start=None, end=None, region=None, program=None):
    return build_dimension_filters(
        start=start,
        end=end,
        region=region,
        program=program,
        year_expression="d.year",
        location_expression="l.state",
        program_expression="p.program_name",
    )


def get_total_students(
    start: int | None = None,
    end: int | None = None,
    region: str | None = None,
    program: str | None = None,
) -> int:
    where_clause, params = _base_where(start, end, region, program)
    row = fetch_one(
        f"""
        SELECT COALESCE(SUM(e.students_total), 0) AS total_students
        FROM fact_exposure e
        LEFT JOIN fact_session_event f ON f.session_key = e.session_key
        LEFT JOIN dim_date d ON d.date_key = f.date_key
        LEFT JOIN dim_location l ON l.location_key = COALESCE(f.location_key, e.location_key)
        LEFT JOIN dim_program p ON p.program_key = COALESCE(f.program_key, e.program_key)
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
    where_clause, params = _base_where(start, end, region, program)
    row = fetch_one(
        f"""
        SELECT
            COALESCE(SUM(e.students_total), 0) AS total_students,
            COALESCE(SUM(COALESCE(e.community_men, 0) + COALESCE(e.community_women, 0)), 0) AS community_members,
            COALESCE(SUM(e.teachers_count), 0) AS teachers_reached,
            COALESCE(AVG(e.students_total), 0) AS avg_students_per_exposure
        FROM fact_exposure e
        LEFT JOIN fact_session_event f ON f.session_key = e.session_key
        LEFT JOIN dim_date d ON d.date_key = f.date_key
        LEFT JOIN dim_location l ON l.location_key = COALESCE(f.location_key, e.location_key)
        LEFT JOIN dim_program p ON p.program_key = COALESCE(f.program_key, e.program_key)
        {where_clause}
        """,
        params,
    )
    return {
        "total_students": int(row.get("total_students", 0) or 0),
        "community_members": int(row.get("community_members", 0) or 0),
        "teachers_reached": int(row.get("teachers_reached", 0) or 0),
        "avg_students_per_exposure": round(float(row.get("avg_students_per_exposure", 0) or 0), 1),
    }


def get_program_metrics(
    start: int | None = None,
    end: int | None = None,
    region: str | None = None,
    program: str | None = None,
    limit: int = 20,
) -> list[dict]:
    where_clause, params = _base_where(start, end, region, program)
    rows = fetch_all(
        f"""
        SELECT
            COALESCE(p.program_name, 'Unknown') AS label,
            COALESCE(SUM(e.students_total), 0) AS value
        FROM fact_exposure e
        LEFT JOIN fact_session_event f ON f.session_key = e.session_key
        LEFT JOIN dim_date d ON d.date_key = f.date_key
        LEFT JOIN dim_location l ON l.location_key = COALESCE(f.location_key, e.location_key)
        LEFT JOIN dim_program p ON p.program_key = COALESCE(f.program_key, e.program_key)
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
    where_clause, params = _base_where(start, end, region, program)
    rows = fetch_all(
        f"""
        SELECT
            COALESCE(p.program_name, 'Unknown') AS label,
            COUNT(*) AS value
        FROM fact_exposure e
        LEFT JOIN fact_session_event f ON f.session_key = e.session_key
        LEFT JOIN dim_date d ON d.date_key = f.date_key
        LEFT JOIN dim_location l ON l.location_key = COALESCE(f.location_key, e.location_key)
        LEFT JOIN dim_program p ON p.program_key = COALESCE(f.program_key, e.program_key)
        {where_clause}
        GROUP BY COALESCE(p.program_name, 'Unknown')
        ORDER BY value DESC, label
        LIMIT 20
        """,
        params,
    )
    return [{"label": row["label"], "value": float(row["value"])} for row in rows]


def get_gender_split(
    start: int | None = None,
    end: int | None = None,
    region: str | None = None,
    program: str | None = None,
) -> dict[str, int]:
    where_clause, params = _base_where(start, end, region, program)
    row = fetch_one(
        f"""
        SELECT
            COALESCE(SUM(e.girls_count), 0) AS girls,
            COALESCE(SUM(e.boys_count), 0) AS boys
        FROM fact_exposure e
        LEFT JOIN fact_session_event f ON f.session_key = e.session_key
        LEFT JOIN dim_date d ON d.date_key = f.date_key
        LEFT JOIN dim_location l ON l.location_key = f.location_key
        LEFT JOIN dim_program p ON p.program_key = f.program_key
        {where_clause}
        """,
        params,
    )
    return {"girls": int(row.get("girls", 0) or 0), "boys": int(row.get("boys", 0) or 0)}


def get_community_gender_split(
    start: int | None = None,
    end: int | None = None,
    region: str | None = None,
    program: str | None = None,
) -> dict[str, int]:
    where_clause, params = _base_where(start, end, region, program)
    row = fetch_one(
        f"""
        SELECT
            COALESCE(SUM(e.community_women), 0) AS women,
            COALESCE(SUM(e.community_men), 0) AS men
        FROM fact_exposure e
        LEFT JOIN fact_session_event f ON f.session_key = e.session_key
        LEFT JOIN dim_date d ON d.date_key = f.date_key
        LEFT JOIN dim_location l ON l.location_key = f.location_key
        LEFT JOIN dim_program p ON p.program_key = f.program_key
        {where_clause}
        """,
        params,
    )
    return {"women": int(row.get("women", 0) or 0), "men": int(row.get("men", 0) or 0)}


def get_top_schools(
    start: int | None = None,
    end: int | None = None,
    region: str | None = None,
    program: str | None = None,
    limit: int = 5,
) -> list[dict]:
    where_clause, params = _base_where(start, end, region, program)
    rows = fetch_all(
        f"""
        SELECT
            COALESCE(NULLIF(l.school_name, ''), 'Unknown school') AS label,
            COALESCE(NULLIF(l.state, ''), 'Unknown') AS state,
            COALESCE(NULLIF(l.district, ''), 'Unknown') AS district,
            COALESCE(SUM(e.students_total), 0) AS value
        FROM fact_exposure e
        LEFT JOIN fact_session_event f ON f.session_key = e.session_key
        LEFT JOIN dim_date d ON d.date_key = f.date_key
        LEFT JOIN dim_location l ON l.location_key = f.location_key
        LEFT JOIN dim_program p ON p.program_key = f.program_key
        {where_clause}
        GROUP BY COALESCE(NULLIF(l.school_name, ''), 'Unknown school'), COALESCE(NULLIF(l.state, ''), 'Unknown'), COALESCE(NULLIF(l.district, ''), 'Unknown')
        ORDER BY value DESC, label
        LIMIT %s
        """,
        [*params, limit],
    )
    return [
        {"label": row["label"], "subtitle": f"{row['state']} - {row['district']}", "value": float(row["value"] or 0)}
        for row in rows
    ]


def get_cohort_breakdown(
    start: int | None = None,
    end: int | None = None,
    region: str | None = None,
    program: str | None = None,
) -> list[dict]:
    where_clause, params = _base_where(start, end, region, program)
    row = fetch_one(
        f"""
        SELECT
            COALESCE(SUM(e.students_total), 0) AS students,
            COALESCE(SUM(e.teachers_count), 0) AS teachers,
            COALESCE(SUM(COALESCE(e.community_men, 0) + COALESCE(e.community_women, 0)), 0) AS community
        FROM fact_exposure e
        LEFT JOIN fact_session_event f ON f.session_key = e.session_key
        LEFT JOIN dim_date d ON d.date_key = f.date_key
        LEFT JOIN dim_location l ON l.location_key = f.location_key
        LEFT JOIN dim_program p ON p.program_key = f.program_key
        {where_clause}
        """,
        params,
    )
    return [
        {"label": "Students", "value": float(row.get("students", 0) or 0)},
        {"label": "Teachers", "value": float(row.get("teachers", 0) or 0)},
        {"label": "Community", "value": float(row.get("community", 0) or 0)},
    ]


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
