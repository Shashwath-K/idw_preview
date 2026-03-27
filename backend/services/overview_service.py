from backend.services.query_utils import build_dimension_filters, fetch_all, fetch_one


LOCATION_EXPRESSION = "l.state"
PROGRAM_EXPRESSION = "p.program_name"


def _build_filters(start: int | None = None, end: int | None = None, region: str | None = None, program: str | None = None):
    return build_dimension_filters(
        start=start,
        end=end,
        region=region,
        program=program,
        year_expression="d.year",
        location_expression=LOCATION_EXPRESSION,
        program_expression=PROGRAM_EXPRESSION,
    )


def get_overview_kpis(start: int | None = None, end: int | None = None, region: str | None = None, program: str | None = None):
    where_clause, params = _build_filters(start=start, end=end, region=region, program=program)

    kpis_row = fetch_one(
        f"""
        SELECT
            COUNT(DISTINCT i.instructor_key) AS total_instructors,
            COUNT(DISTINCT l.state) AS total_states,
            COUNT(DISTINCT p.program_name) AS total_programs
        FROM fact_session_event f
        LEFT JOIN dim_date d ON d.date_key = f.date_key
        LEFT JOIN dim_location l ON l.location_key = f.location_key
        LEFT JOIN dim_program p ON p.program_key = f.program_key
        LEFT JOIN dim_instructor i ON i.instructor_key = f.instructor_key
        {where_clause}
        """,
        params,
    )

    return {
        "total_instructors": int(kpis_row.get("total_instructors", 0) or 0),
        "total_drivers": 0,  # Placeholder until driver schema is added
        "total_states": int(kpis_row.get("total_states", 0) or 0),
        "total_programs": int(kpis_row.get("total_programs", 0) or 0),
    }

def get_overview_charts(start: int | None = None, end: int | None = None, region: str | None = None, program: str | None = None):
    where_clause, params = _build_filters(start=start, end=end, region=region, program=program)
    
    # 1. Instructors per region
    instructors_rows = fetch_all(
        f"""
        SELECT
            COALESCE(l.state, 'Unknown') AS label,
            COUNT(DISTINCT i.instructor_key) AS value
        FROM fact_session_event f
        LEFT JOIN dim_date d ON d.date_key = f.date_key
        LEFT JOIN dim_location l ON l.location_key = f.location_key
        LEFT JOIN dim_program p ON p.program_key = f.program_key
        LEFT JOIN dim_instructor i ON i.instructor_key = f.instructor_key
        {where_clause} AND l.state IS NOT NULL
        GROUP BY l.state
        ORDER BY value DESC
        LIMIT 10
        """,
        params,
    )
    
    # 2. Programs per region
    programs_rows = fetch_all(
        f"""
        SELECT
            COALESCE(l.state, 'Unknown') AS label,
            COUNT(DISTINCT p.program_name) AS value
        FROM fact_session_event f
        LEFT JOIN dim_date d ON d.date_key = f.date_key
        LEFT JOIN dim_location l ON l.location_key = f.location_key
        LEFT JOIN dim_program p ON p.program_key = f.program_key
        {where_clause} AND l.state IS NOT NULL
        GROUP BY l.state
        ORDER BY value DESC
        LIMIT 10
        """,
        params,
    )

    return {
        "instructors_by_region": [{"label": r["label"], "value": float(r["value"])} for r in instructors_rows],
        "drivers_by_region": [], # Placeholder
        "programs_by_region": [{"label": r["label"], "value": float(r["value"])} for r in programs_rows]
    }


def get_program_targets(start: int | None = None, end: int | None = None, region: str | None = None, program: str | None = None, limit: int = 10, offset: int = 0):
    where_clause, params = _build_filters(start=start, end=end, region=region, program=program)
    
    # Get total count
    count_sql = f"""
        SELECT COUNT(*) FROM (
            SELECT p.program_key
            FROM fact_session_event f
            LEFT JOIN dim_date d ON d.date_key = f.date_key
            LEFT JOIN dim_location l ON l.location_key = f.location_key
            LEFT JOIN dim_program p ON p.program_key = f.program_key
            {where_clause}
            GROUP BY p.program_key
            HAVING p.program_key IS NOT NULL
        ) as sub
    """
    total_count = fetch_one(count_sql, params)["count"]

    rows = fetch_all(
        f"""
        SELECT
            p.program_key,
            COALESCE(MAX(p.program_name), 'Unknown') AS label,
            COALESCE(MAX(dnr.donor_name), 'Unknown') AS donor,
            COALESCE(MAX(p.target_sessions), 0) AS target_sessions,
            COALESCE(SUM(f.session_count), 0) AS completed_sessions,
            COALESCE(MAX(p.target_students), 0) AS target_students,
            COALESCE(SUM(e.students_total), 0) AS reached_students,
            MAX(p.end_date) AS end_date
        FROM fact_session_event f
        LEFT JOIN fact_exposure e ON e.session_key = f.session_key
        LEFT JOIN dim_date d ON d.date_key = f.date_key
        LEFT JOIN dim_location l ON l.location_key = f.location_key
        LEFT JOIN dim_program p ON p.program_key = f.program_key
        LEFT JOIN dim_donor dnr ON dnr.donor_key = p.donor_key
        {where_clause}
        GROUP BY p.program_key
        HAVING p.program_key IS NOT NULL
        ORDER BY completed_sessions DESC, label
        LIMIT %s OFFSET %s
        """,
        [*params, limit, offset],
    )

    items = []
    for row in rows:
        target_sessions = int(row.get("target_sessions", 0) or 0)
        completed_sessions = int(row.get("completed_sessions", 0) or 0)
        pct = round((completed_sessions / target_sessions) * 100) if target_sessions else 0
        if pct >= 80:
            status = "On track"
        elif pct >= 50:
            status = "At risk"
        else:
            status = "Behind"
        items.append(
            {
                "label": row.get("label") or "Unknown",
                "donor": row.get("donor") or "Unknown",
                "completed_sessions": completed_sessions,
                "target_sessions": target_sessions,
                        "students_target": int(row.get("target_students", 0) or 0),
                        "students_reached": int(row.get("reached_students", 0) or 0),
                "progress_pct": pct,
                "end_date": row["end_date"].strftime("%b %Y") if row.get("end_date") else "Unknown",
                "status": status,
            }
        )
    return {"table": items, "total_count": total_count}


def get_sessions_by_activity(start: int | None = None, end: int | None = None, region: str | None = None, program: str | None = None):
    where_clause, params = _build_filters(start=start, end=end, region=region, program=program)
    rows = fetch_all(
        f"""
        SELECT
            COALESCE(a.activity_name, 'Unknown') AS label,
            COALESCE(SUM(f.session_count), 0) AS value
        FROM fact_session_event f
        LEFT JOIN dim_date d ON d.date_key = f.date_key
        LEFT JOIN dim_location l ON l.location_key = f.location_key
        LEFT JOIN dim_program p ON p.program_key = f.program_key
        LEFT JOIN dim_activity a ON a.activity_key = f.activity_key
        {where_clause}
        GROUP BY COALESCE(a.activity_name, 'Unknown')
        ORDER BY value DESC, label
        LIMIT 6
        """,
        params,
    )
    return [{"label": row["label"], "value": float(row["value"])} for row in rows]


def get_sessions_by_donor(start: int | None = None, end: int | None = None, region: str | None = None, program: str | None = None):
    where_clause, params = _build_filters(start=start, end=end, region=region, program=program)
    rows = fetch_all(
        f"""
        SELECT
            COALESCE(dnr.donor_name, 'Unknown') AS label,
            COALESCE(SUM(f.session_count), 0) AS value
        FROM fact_session_event f
        LEFT JOIN dim_date d ON d.date_key = f.date_key
        LEFT JOIN dim_location l ON l.location_key = f.location_key
        LEFT JOIN dim_program p ON p.program_key = f.program_key
        LEFT JOIN dim_donor dnr ON dnr.donor_key = p.donor_key
        {where_clause}
        GROUP BY COALESCE(dnr.donor_name, 'Unknown')
        ORDER BY value DESC, label
        LIMIT 6
        """,
        params,
    )
    return [{"label": row["label"], "value": float(row["value"])} for row in rows]
