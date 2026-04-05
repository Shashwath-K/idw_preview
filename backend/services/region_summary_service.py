from backend.services.query_utils import fetch_all, fetch_one


def get_region_summary_filters():
    # Fetch from new dim_program and dim_date
    programs = [row["program_category"] for row in fetch_all("SELECT DISTINCT program_category FROM dw.dim_program WHERE program_category IS NOT NULL ORDER BY program_category")]
    
    years = [row["year_actual"] for row in fetch_all("SELECT DISTINCT year_actual FROM dw.dim_date ORDER BY year_actual DESC")]
    
    months = [{"id": row["month_actual"], "name": row["month_name"].strip()} for row in fetch_all("SELECT DISTINCT month_actual, TO_CHAR(TO_DATE(month_actual::text, 'MM'), 'Month') as month_name FROM dw.dim_date ORDER BY month_actual")]
    
    return {
        "programs": programs,
        "years": years,
        "months": months
    }


def get_region_summary_data(program_type=None, year=None, month=None, limit=15, offset=0):
    where_clauses = ["TRUE"]
    params = []
    
    if program_type:
        where_clauses.append("p.program_category = %s")
        params.append(program_type)
    if year:
        where_clauses.append("d.year_actual = %s")
        params.append(int(year))
    if month:
        where_clauses.append("d.month_actual = %s")
        params.append(int(month))
    
    where_sql = " AND ".join(where_clauses)
    
    # Get total count
    count_sql = f"""
        SELECT COUNT(DISTINCT g.region_name)
        FROM dw.fact_session f
        JOIN dw.dim_geography g ON f.sk_geography_id = g.sk_geography_id
        JOIN dw.dim_date d ON f.date_id = d.date_id
        JOIN dw.dim_program p ON f.sk_program_id = p.sk_program_id
        WHERE {where_sql}
    """
    total_count = fetch_one(count_sql, params).get("count", 0)

    # Main data query
    main_sql = f"""
        SELECT 
            g.region_name as region,
            COALESCE(SUM(f.session_count), 0) as sessions,
            COALESCE(SUM(e.total_students), 0) as students_reached,
            COALESCE(SUM(e.total_teachers), 0) as teachers_trained,
            COUNT(DISTINCT e.sk_school_id) as schools_covered,
            COALESCE(SUM(e.community_total), 0) as community_reach
        FROM dw.fact_session f
        JOIN dw.dim_geography g ON f.sk_geography_id = g.sk_geography_id
        JOIN dw.dim_date d ON f.date_id = d.date_id
        JOIN dw.dim_program p ON f.sk_program_id = p.sk_program_id
        LEFT JOIN dw.fact_attendance_exposure e ON f.sk_session_id = e.sk_session_id
        WHERE {where_sql}
        GROUP BY g.region_name
        ORDER BY g.region_name
        LIMIT %s OFFSET %s
    """
    table_data = fetch_all(main_sql, params + [limit, offset])
    
    # Aggregated KPIs
    kpi_sql = f"""
        SELECT 
            SUM(CASE WHEN (p.program_name ILIKE '%%STEM for Schools%%' OR p.program_name ILIKE '%%School%%') THEN f.session_count ELSE 0 END) as school_visits,
            SUM(CASE WHEN a.activity_name ILIKE '%%Fair%%' THEN f.session_count ELSE 0 END) as sf_count,
            SUM(CASE WHEN (p.program_name ILIKE '%%Community%%' OR a.activity_name ILIKE '%%CV%%') THEN f.session_count ELSE 0 END) as cv_count,
            SUM(CASE WHEN (p.program_name ILIKE '%%Teacher%%' OR a.activity_name ILIKE '%%Training%%') THEN f.session_count ELSE 0 END) as ttp_count
        FROM dw.fact_session f
        JOIN dw.dim_geography g ON f.sk_geography_id = g.sk_geography_id
        JOIN dw.dim_date d ON f.date_id = d.date_id
        JOIN dw.dim_program p ON f.sk_program_id = p.sk_program_id
        JOIN dw.dim_activity_type a ON f.sk_activity_type_id = a.sk_activity_type_id
        WHERE {where_sql}
    """
    totals = fetch_one(kpi_sql, params)
    
    return {
        "kpis": [
            {"label": "Total School visits", "value": int(totals.get("school_visits", 0) or 0), "icon": "fas fa-school"},
            {"label": "Total Science Fair (SF)", "value": int(totals.get("sf_count", 0) or 0), "icon": "fas fa-flask"},
            {"label": "Total Community Visit (CV)", "value": int(totals.get("cv_count", 0) or 0), "icon": "fas fa-users"},
            {"label": "Total Teacher Training Program (TTP)", "value": int(totals.get("ttp_count", 0) or 0), "icon": "fas fa-chalkboard-teacher"},
        ],
        "table": table_data,
        "total_count": total_count
    }

