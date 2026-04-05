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
        year_expression="d.year_actual",
        location_expression="g.region_name",
        program_expression="p.program_name",
    )

    row = fetch_one(
        f"""
        SELECT
            (SELECT COALESCE(SUM(total_exposure_count), 0) FROM dw.fact_attendance_exposure fae
             LEFT JOIN dw.dim_date d ON d.date_id = fae.date_id
             LEFT JOIN dw.dim_geography g ON g.sk_geography_id = fae.sk_geography_id
             LEFT JOIN dw.dim_program p ON p.sk_program_id = fae.sk_program_id
             {where_clause}) AS total_students_reached,
            COUNT(DISTINCT g.nk_region_id) AS total_states,
            COUNT(f.sk_fact_session_id) AS total_sessions,
            COALESCE(
                (SELECT SUM(total_exposure_count) FROM dw.fact_attendance_exposure fae {where_clause.replace('g.', 'ge.') if where_clause else ''}) / NULLIF(COUNT(DISTINCT g.nk_region_id), 0),
                0
            ) AS avg_students_per_state_period
        FROM dw.fact_session f
        LEFT JOIN dw.dim_date d ON d.date_id = f.date_id
        LEFT JOIN dw.dim_geography g ON g.sk_geography_id = f.sk_geography_id
        LEFT JOIN dw.dim_program p ON p.sk_program_id = f.sk_program_id
        {where_clause}
        """,
        params,
    )
    # Note: The avg students per region logic in the original was complex, 
    # simplified here to average across matched regions.

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
        year_expression="d.year_actual",
        location_expression="g.region_name",
        program_expression="p.program_name",
    )

    rows = fetch_all(
        f"""
        SELECT
            COALESCE(g.region_name, 'Unknown') AS label,
            COALESCE(SUM(fae.total_exposure_count), 0) AS value
        FROM dw.fact_attendance_exposure fae
        LEFT JOIN dw.dim_date d ON d.date_id = fae.date_id
        LEFT JOIN dw.dim_geography g ON g.sk_geography_id = fae.sk_geography_id
        LEFT JOIN dw.dim_program p ON p.sk_program_id = fae.sk_program_id
        {where_clause}
        GROUP BY g.region_name
        ORDER BY value DESC, label
        LIMIT 20
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
        year_expression="d.year_actual",
        location_expression="g.region_name",
        program_expression="p.program_name",
    )

    rows = fetch_all(
        f"""
        SELECT
            TO_CHAR(DATE_TRUNC('month', d.full_date), 'YYYY-MM') AS label,
            COALESCE(SUM(fae.total_exposure_count), 0) AS value,
            DATE_TRUNC('month', d.full_date) AS sort_key
        FROM dw.fact_attendance_exposure fae
        LEFT JOIN dw.dim_date d ON d.date_id = fae.date_id
        LEFT JOIN dw.dim_geography g ON g.sk_geography_id = fae.sk_geography_id
        LEFT JOIN dw.dim_program p ON p.sk_program_id = fae.sk_program_id
        {where_clause}
        GROUP BY DATE_TRUNC('month', d.full_date)
        ORDER BY sort_key
        """,
        params,
    )
    return [{"label": row["label"], "value": float(row["value"])} for row in rows]



def get_region_options() -> list[str]:
    # Strictly return states (Andhra, Karnataka, etc.) as requested
    rows = fetch_all(
        """
        SELECT DISTINCT region_name as state
        FROM dw.dim_geography
        WHERE region_name IS NOT NULL 
        ORDER BY region_name
        """
    )

    return [str(row["state"]) for row in rows if row.get("state")]
