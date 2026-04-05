from backend.services.query_utils import fetch_all, fetch_one


def get_instructor_summary_filters():
    # Fetch from new dim_geography and dim_date
    locations = fetch_all("SELECT DISTINCT region_name, district AS area FROM dw.dim_geography WHERE region_name IS NOT NULL ORDER BY region_name, district")
    
    years = [row["year_actual"] for row in fetch_all("SELECT DISTINCT year_actual FROM dw.dim_date WHERE year_actual IS NOT NULL ORDER BY year_actual DESC")]
    
    months = [{"id": row["month_actual"], "name": row["month_name"].strip()} for row in fetch_all("SELECT DISTINCT month_actual, TO_CHAR(TO_DATE(month_actual::text, 'MM'), 'Month') as month_name FROM dw.dim_date ORDER BY month_actual")]
    
    return {
        "regions": sorted(list(set(row["region_name"] for row in locations))),
        "areas": sorted(list(set(row["area"] for row in locations if row.get("area")))),
        "years": years,
        "months": months
    }


def get_instructor_summary_data(region=None, area=None, year=None, month=None, limit=15, offset=0):
    where_clauses = ["TRUE"]
    params = []
    
    if region:
        where_clauses.append("g.region_name = %s")
        params.append(region)
    if area:
        where_clauses.append("g.district = %s")
        params.append(area)
    if year:
        where_clauses.append("d.year_actual = %s")
        params.append(int(year))
    if month:
        where_clauses.append("d.month_actual = %s")
        params.append(int(month))
    
    where_sql = " AND ".join(where_clauses)
    
    # Get KPIs
    kpi_sql = f"""
        SELECT 
            COUNT(DISTINCT d.full_date) as days_worked,
            SUM(COALESCE(f.session_count, 0)) as total_sessions,
            SUM(COALESCE(e.total_students, 0)) as total_exposures,
            SUM(CASE WHEN a.activity_name ILIKE ANY (ARRAY['%%YIL%%', '%%SF%%', '%%CV%%']) THEN COALESCE(e.total_students, 0) ELSE 0 END) as combined_exposures
        FROM dw.dim_user u
        LEFT JOIN dw.fact_session f ON u.sk_user_id = f.sk_user_id
        LEFT JOIN dw.fact_attendance_exposure e ON f.sk_session_id = e.sk_session_id
        LEFT JOIN dw.dim_date d ON f.date_id = d.date_id
        LEFT JOIN dw.dim_geography g ON f.sk_geography_id = g.sk_geography_id
        LEFT JOIN dw.dim_activity_type a ON f.sk_activity_type_id = a.sk_activity_type_id
        WHERE {where_sql}
    """
    kpi_res = fetch_one(kpi_sql, params)
    
    total_exp = int(kpi_res["total_exposures"] or 0)
    comb_exp = int(kpi_res["combined_exposures"] or 0)
    
    kpis = {
        "days_worked": kpi_res["days_worked"] or 0,
        "total_sessions": int(kpi_res["total_sessions"] or 0),
        "school_exposures": total_exp - comb_exp,
        "combined_exposures": comb_exp
    }

    # Get total count for pagination
    count_sql = f"""
        SELECT COUNT(*) FROM (
            SELECT u.sk_user_id
            FROM dw.dim_user u
            JOIN dw.fact_session f ON u.sk_user_id = f.sk_user_id
            JOIN dw.dim_geography g ON f.sk_geography_id = g.sk_geography_id
            JOIN dw.dim_date d ON f.date_id = d.date_id
            WHERE {where_sql}
            GROUP BY u.sk_user_id
        ) as sub
    """
    total_count = fetch_one(count_sql, params).get("count", 0)

    # Main query to aggregate instructor metrics
    main_sql = f"""
        SELECT 
            u.full_name as instructor_name,
            COUNT(DISTINCT d.full_date) as days_worked,
            COUNT(DISTINCT f.sk_session_id) as school_sessions,
            SUM(COALESCE(f.session_count, 0)) as total_sessions,
            SUM(COALESCE(e.total_students, 0)) as total_exposures,
            COUNT(DISTINCT CASE WHEN a.activity_name ILIKE '%%Fair%%' THEN f.sk_session_id END) as fair_count,
            SUM(CASE WHEN a.activity_name ILIKE '%%Training%%' THEN COALESCE(e.total_students, 0) ELSE 0 END) as training_exposures,
            SUM(CASE WHEN a.activity_name ILIKE '%%SF%%' THEN COALESCE(e.total_students, 0) ELSE 0 END) as sf_exposures,
            COUNT(DISTINCT CASE WHEN a.activity_name ILIKE '%%YIL%%' THEN f.sk_session_id END) as yil_sessions,
            SUM(CASE WHEN a.activity_name ILIKE '%%YIL%%' THEN COALESCE(e.total_students, 0) ELSE 0 END) as yil_exposures,
            COUNT(DISTINCT CASE WHEN a.activity_name ILIKE '%%CV%%' THEN f.sk_session_id END) as cv_visits,
            SUM(CASE WHEN a.activity_name ILIKE '%%CV%%' THEN COALESCE(e.total_students, 0) ELSE 0 END) as cv_exposures
        FROM dw.dim_user u
        LEFT JOIN dw.fact_session f ON u.sk_user_id = f.sk_user_id
        LEFT JOIN dw.fact_attendance_exposure e ON f.sk_session_id = e.sk_session_id
        LEFT JOIN dw.dim_date d ON f.date_id = d.date_id
        LEFT JOIN dw.dim_geography g ON f.sk_geography_id = g.sk_geography_id
        LEFT JOIN dw.dim_activity_type a ON f.sk_activity_type_id = a.sk_activity_type_id
        WHERE {where_sql}
        GROUP BY u.sk_user_id, u.full_name
        ORDER BY u.full_name
        LIMIT %s OFFSET %s
    """
    table_data = fetch_all(main_sql, params + [limit, offset])
    
    return {
        "table": table_data,
        "total_count": total_count,
        "kpis": kpis
    }

