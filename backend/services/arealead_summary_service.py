from backend.services.query_utils import fetch_all, fetch_one


def get_arealead_summary_filters():
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


def get_arealead_summary_data(region=None, area=None, year=None, month=None, limit=15, offset=0):
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
    
    # Get total count
    count_sql = f"""
        SELECT COUNT(*) FROM (
            SELECT g.district, g.region_name
            FROM dw.fact_session f
            JOIN dw.dim_geography g ON f.sk_geography_id = g.sk_geography_id
            JOIN dw.dim_user u ON f.sk_user_id = u.sk_user_id
            JOIN dw.dim_date d ON f.date_id = d.date_id
            WHERE {where_sql}
            GROUP BY g.district, g.region_name
        ) as sub
    """
    total_count = fetch_one(count_sql, params).get("count", 0)

    # Get paginated data
    sql = f"""
        SELECT 
            g.district as area,
            g.region_name as region,
            COUNT(DISTINCT u.sk_user_id) as total_instructors,
            COUNT(DISTINCT f.sk_session_id) as total_sessions,
            SUM(COALESCE(e.total_students, 0)) as total_students
        FROM dw.fact_session f
        JOIN dw.dim_geography g ON f.sk_geography_id = g.sk_geography_id
        JOIN dw.dim_user u ON f.sk_user_id = u.sk_user_id
        JOIN dw.dim_date d ON f.date_id = d.date_id
        LEFT JOIN dw.fact_attendance_exposure e ON f.sk_session_id = e.sk_session_id
        WHERE {where_sql}
        GROUP BY g.district, g.region_name
        ORDER BY g.region_name, g.district
        LIMIT %s OFFSET %s
    """
    rows = fetch_all(sql, params + [limit, offset])
    return {"table": rows, "total_count": total_count}

