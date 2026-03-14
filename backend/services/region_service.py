from backend.services.query_utils import build_dimension_filters, fetch_all, fetch_one


MONTH_LABEL = """
CASE f.month_key
    WHEN 1 THEN 'Jan'
    WHEN 2 THEN 'Feb'
    WHEN 3 THEN 'Mar'
    WHEN 4 THEN 'Apr'
    WHEN 5 THEN 'May'
    WHEN 6 THEN 'Jun'
    WHEN 7 THEN 'Jul'
    WHEN 8 THEN 'Aug'
    WHEN 9 THEN 'Sep'
    WHEN 10 THEN 'Oct'
    WHEN 11 THEN 'Nov'
    WHEN 12 THEN 'Dec'
    ELSE CONCAT('Month ', f.month_key::text)
END
"""


def get_region_kpis(
    start: int | None = None,
    end: int | None = None,
    region: str | None = None,
    program: str | None = None,
) -> dict[str, float]:
    where_clause, params = build_dimension_filters(
        start=None,
        end=None,
        region=region,
        program=None,
        location_expression="l.state",
    )
    row = fetch_one(
        f"""
        SELECT
            COALESCE(SUM(f.students_reached), 0) AS total_students_reached,
            COUNT(DISTINCT l.state) AS total_states,
            COALESCE(SUM(f.sessions), 0) AS total_sessions,
            COALESCE(AVG(f.students_reached), 0) AS avg_students_per_state_period
        FROM fact_monthly_region_impact f
        LEFT JOIN dim_location l ON l.location_key = f.location_key
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
        start=None,
        end=None,
        region=region,
        program=None,
        location_expression="l.state",
    )
    rows = fetch_all(
        f"""
        SELECT
            COALESCE(l.state, 'Unknown') AS label,
            COALESCE(SUM(f.students_reached), 0) AS value
        FROM fact_monthly_region_impact f
        LEFT JOIN dim_location l ON l.location_key = f.location_key
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
        start=None,
        end=None,
        region=region,
        program=None,
        location_expression="l.state",
    )
    rows = fetch_all(
        f"""
        SELECT
            {MONTH_LABEL} AS label,
            COALESCE(SUM(f.students_reached), 0) AS value,
            f.month_key AS sort_key
        FROM fact_monthly_region_impact f
        LEFT JOIN dim_location l ON l.location_key = f.location_key
        {where_clause}
        GROUP BY f.month_key
        ORDER BY f.month_key
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
