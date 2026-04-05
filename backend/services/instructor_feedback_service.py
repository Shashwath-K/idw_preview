from backend.services.query_utils import fetch_all, fetch_one


def get_instructor_feedback_filters():
    # Fetch from new dim_user and dim_date
    instructors = [row["full_name"] for row in fetch_all("SELECT DISTINCT full_name FROM dw.dim_user WHERE full_name IS NOT NULL ORDER BY full_name")]
    
    years = [row["year_actual"] for row in fetch_all("SELECT DISTINCT year_actual FROM dw.dim_date WHERE year_actual IS NOT NULL ORDER BY year_actual DESC")]
    
    return {
        "instructors": instructors,
        "years": years
    }


def get_instructor_feedback_data(instructor_name=None, year=None, limit=15, offset=0):
    where_clauses = ["TRUE"]
    params = []
    
    if instructor_name:
        where_clauses.append("u.full_name = %s")
        params.append(instructor_name)
    if year:
        where_clauses.append("d.year_actual = %s")
        params.append(int(year))
    
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

    # Main query (partially placeholder logic)
    sql = f"""
        SELECT 
            u.full_name as instructor_name,
            d.full_date as date,
            a.activity_name,
            '4.5/5' as rating, -- Placeholder
            'Positive observation' as comments -- Placeholder
        FROM dw.fact_session f
        JOIN dw.dim_user u ON f.sk_user_id = u.sk_user_id
        JOIN dw.dim_date d ON f.date_id = d.date_id
        JOIN dw.dim_activity_type a ON f.sk_activity_type_id = a.sk_activity_type_id
        WHERE {where_sql}
        ORDER BY d.full_date DESC
        LIMIT %s OFFSET %s
    """
    rows = fetch_all(sql, params + [limit, offset])
    
    return {
        "table": [{**row, "date": row["date"].strftime("%Y-%m-%d") if row["date"] else None} for row in rows],
        "total_count": total_count
    }

            
