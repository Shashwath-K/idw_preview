from backend.services.query_utils import fetch_all, fetch_one


def get_instructor_detail_filters():
    # Fetch from new dim_user and dim_date
    instructors = [row["full_name"] for row in fetch_all("SELECT DISTINCT full_name FROM dw.dim_user WHERE full_name IS NOT NULL ORDER BY full_name")]
    
    years = [row["year_actual"] for row in fetch_all("SELECT DISTINCT year_actual FROM dw.dim_date WHERE year_actual IS NOT NULL ORDER BY year_actual DESC")]
    
    months = [{"id": row["month_actual"], "name": row["month_name"].strip()} for row in fetch_all("SELECT DISTINCT month_actual, TO_CHAR(TO_DATE(month_actual::text, 'MM'), 'Month') as month_name FROM dw.dim_date ORDER BY month_actual")]
    
    return {
        "instructors": instructors,
        "years": years,
        "months": months
    }


def get_instructor_detail_data(instructor_name=None, year=None, month=None, limit=15, offset=0):
    where_clauses = ["TRUE"]
    params = []
    
    if instructor_name:
        where_clauses.append("u.full_name = %s")
        params.append(instructor_name)
    if year:
        where_clauses.append("d.year_actual = %s")
        params.append(int(year))
    if month:
        where_clauses.append("d.month_actual = %s")
        params.append(int(month))
    
    where_sql = " AND ".join(where_clauses)
    
    # Get total count
    count_sql = f"""
        SELECT COUNT(*)
        FROM dw.fact_session f
        JOIN dw.dim_user u ON f.sk_user_id = u.sk_user_id
        JOIN dw.dim_date d ON f.date_id = d.date_id
        WHERE {where_sql}
    """
    total_count = fetch_one(count_sql, params).get("count", 0)

    # Get paginated data
    sql = f"""
        SELECT 
            d.full_date as date,
            s.school_name,
            a.activity_name,
            f.session_duration,
            COALESCE(e.total_students, 0) as students,
            CASE WHEN f.is_overdue THEN 'Overdue' ELSE 'Completed' END as status
        FROM dw.fact_session f
        JOIN dw.dim_user u ON f.sk_user_id = u.sk_user_id
        JOIN dw.dim_date d ON f.date_id = d.date_id
        JOIN dw.dim_school s ON f.sk_school_id = s.sk_school_id
        JOIN dw.dim_activity_type a ON f.sk_activity_type_id = a.sk_activity_type_id
        LEFT JOIN dw.fact_attendance_exposure e ON f.sk_session_id = e.sk_session_id
        WHERE {where_sql}
        ORDER BY d.full_date DESC
        LIMIT %s OFFSET %s
    """
    rows = fetch_all(sql, params + [limit, offset])
    
    return {
        "table": [{**row, "date": row["date"].strftime("%Y-%m-%d") if row["date"] else None} for row in rows],
        "total_count": total_count
    }

