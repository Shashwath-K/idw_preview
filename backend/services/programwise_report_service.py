from backend.services.query_utils import fetch_all, fetch_one


def get_programwise_report_filters():
    # Fetch from new dim_program and dim_date
    categories = [row["program_category"] for row in fetch_all("SELECT DISTINCT program_category FROM dw.dim_program WHERE program_category IS NOT NULL ORDER BY program_category")]
    
    years = [row["year_actual"] for row in fetch_all("SELECT DISTINCT year_actual FROM dw.dim_date WHERE year_actual IS NOT NULL ORDER BY year_actual DESC")]
    
    months = [{"id": row["month_actual"], "name": row["month_name"].strip()} for row in fetch_all("SELECT DISTINCT month_actual, TO_CHAR(TO_DATE(month_actual::text, 'MM'), 'Month') as month_name FROM dw.dim_date ORDER BY month_actual")]
    
    return {
        "categories": categories,
        "years": years,
        "months": months
    }


def get_programwise_report_data(category=None, year=None, month=None, limit=15, offset=0):
    where_clauses = ["TRUE"]
    params = []
    
    if category:
        where_clauses.append("p.program_category = %s")
        params.append(category)
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
            SELECT p.program_name
            FROM dw.fact_session f
            JOIN dw.dim_program p ON f.sk_program_id = p.sk_program_id
            JOIN dw.dim_date d ON f.date_id = d.date_id
            WHERE {where_sql}
            GROUP BY p.sk_program_id, p.program_name, p.program_category
        ) as sub
    """
    total_count = fetch_one(count_sql, params).get("count", 0)

    # Get paginated data
    sql = f"""
        SELECT 
            p.program_name,
            p.program_category,
            COUNT(DISTINCT f.sk_session_id) as sessions,
            SUM(COALESCE(e.total_students, 0)) as students,
            COUNT(DISTINCT f.sk_school_id) as schools_covered
        FROM dw.fact_session f
        JOIN dw.dim_program p ON f.sk_program_id = p.sk_program_id
        JOIN dw.dim_date d ON f.date_id = d.date_id
        LEFT JOIN dw.fact_attendance_exposure e ON f.sk_session_id = e.sk_session_id
        WHERE {where_sql}
        GROUP BY p.sk_program_id, p.program_name, p.program_category
        ORDER BY sessions DESC
        LIMIT %s OFFSET %s
    """
    rows = fetch_all(sql, params + [limit, offset])
    return {"table": rows, "total_count": total_count}

