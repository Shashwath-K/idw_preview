from backend.services.query_utils import fetch_all, fetch_one


def get_school_visit_filters():
    # Fetch from new dim_geography, dim_program and dim_date
    locations = fetch_all("SELECT DISTINCT region_name, district AS area FROM dw.dim_geography WHERE region_name IS NOT NULL ORDER BY region_name, district")
    
    programs = [row["program_name"] for row in fetch_all("SELECT DISTINCT program_name FROM dw.dim_program WHERE program_name IS NOT NULL ORDER BY program_name")]
    
    years = [row["year_actual"] for row in fetch_all("SELECT DISTINCT year_actual FROM dw.dim_date WHERE year_actual IS NOT NULL ORDER BY year_actual DESC")]
    
    months = [{"id": row["month_actual"], "name": row["month_name"].strip()} for row in fetch_all("SELECT DISTINCT month_actual, TO_CHAR(TO_DATE(month_actual::text, 'MM'), 'Month') as month_name FROM dw.dim_date ORDER BY month_actual")]
    
    return {
        "regions": sorted(list(set(row["region_name"] for row in locations))),
        "areas": sorted(list(set(row["area"] for row in locations if row.get("area")))),
        "programs": programs,
        "years": years,
        "months": months
    }


def get_school_visit_data(region=None, area=None, program=None, year=None, month=None, limit=15, offset=0):
    where_clauses = ["TRUE"]
    params = []
    
    if region:
        where_clauses.append("g.region_name = %s")
        params.append(region)
    if area:
        where_clauses.append("g.district = %s")
        params.append(area)
    if program:
        where_clauses.append("p.program_name = %s")
        params.append(program)
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
            COUNT(DISTINCT f.sk_school_id) as total_schools,
            SUM(f.session_count) as total_sessions,
            SUM(COALESCE(e.total_students, 0)) as total_students
        FROM dw.fact_session f
        JOIN dw.dim_geography g ON f.sk_geography_id = g.sk_geography_id
        JOIN dw.dim_program p ON f.sk_program_id = p.sk_program_id
        JOIN dw.dim_date d ON f.date_id = d.date_id
        LEFT JOIN dw.fact_attendance_exposure e ON f.sk_session_id = e.sk_session_id
        WHERE {where_sql}
    """
    kpi_res = fetch_one(kpi_sql, params)

    # Monthly Sessions (sessions in the latest month of the selected period)
    monthly_sql = f"""
        SELECT SUM(session_count) as monthly_sessions
        FROM dw.fact_session f
        JOIN dw.dim_date d ON f.date_id = d.date_id
        JOIN dw.dim_geography g ON f.sk_geography_id = g.sk_geography_id
        JOIN dw.dim_program p ON f.sk_program_id = p.sk_program_id
        WHERE {where_sql}
        AND d.month_actual = (
            SELECT MAX(d2.month_actual) 
            FROM dw.fact_session f2
            JOIN dw.dim_date d2 ON f2.date_id = d2.date_id
            JOIN dw.dim_geography g2 ON f2.sk_geography_id = g2.sk_geography_id
            JOIN dw.dim_program p2 ON f2.sk_program_id = p2.sk_program_id
            WHERE {where_sql}
        )
    """
    monthly_res = fetch_one(monthly_sql, params + params)

    kpis = {
        "total_schools": kpi_res.get("total_schools") or 0,
        "total_students": int(kpi_res.get("total_students") or 0),
        "total_sessions": int(kpi_res.get("total_sessions") or 0),
        "monthly_sessions": int(monthly_res.get("monthly_sessions") or 0) if monthly_res else 0
    }

    # Get total row count for pagination
    count_sql = f"""
        SELECT COUNT(*) FROM (
            SELECT s.school_name
            FROM dw.fact_session f
            JOIN dw.dim_school s ON f.sk_school_id = s.sk_school_id
            JOIN dw.dim_program p ON f.sk_program_id = p.sk_program_id
            JOIN dw.dim_geography g ON f.sk_geography_id = g.sk_geography_id
            JOIN dw.dim_date d ON f.date_id = d.date_id
            WHERE {where_sql}
            GROUP BY s.school_name, p.program_name, g.region_name, g.district
        ) as sub
    """
    total_count = fetch_one(count_sql, params).get("count", 0)

    # Get paginated data
    sql = f"""
        SELECT 
            s.school_name,
            p.program_name,
            g.region_name as region,
            g.district as area,
            SUM(f.session_count) as sessions,
            SUM(COALESCE(e.total_students, 0)) as students
        FROM dw.fact_session f
        JOIN dw.dim_school s ON f.sk_school_id = s.sk_school_id
        JOIN dw.dim_program p ON f.sk_program_id = p.sk_program_id
        JOIN dw.dim_geography g ON f.sk_geography_id = g.sk_geography_id
        JOIN dw.dim_date d ON f.date_id = d.date_id
        LEFT JOIN dw.fact_attendance_exposure e ON f.sk_session_id = e.sk_session_id
        WHERE {where_sql}
        GROUP BY s.school_name, p.program_name, g.region_name, g.district
        ORDER BY sessions DESC
        LIMIT %s OFFSET %s
    """
    rows = fetch_all(sql, params + [limit, offset])
    
    return {
        "table": rows, 
        "total_count": total_count,
        "kpis": kpis
    }

