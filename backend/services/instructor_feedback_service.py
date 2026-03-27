from backend.db import get_datamart_conn

def get_instructor_feedback_filters():
    with get_datamart_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT name FROM dw_data_schema.dim_instructor WHERE name IS NOT NULL ORDER BY name")
            instructors = [row["name"] for row in cur.fetchall()]
            
            cur.execute("SELECT DISTINCT financial_year FROM dw_data_schema.dim_date WHERE financial_year IS NOT NULL ORDER BY financial_year DESC")
            years = [row["financial_year"] for row in cur.fetchall()]
            
    return {
        "instructors": instructors,
        "years": years
    }

def get_instructor_feedback_data(instructor_name=None, year=None, limit=15, offset=0):
    with get_datamart_conn() as conn:
        with conn.cursor() as cur:
            where_clauses = ["TRUE"]
            params = []
            if instructor_name:
                where_clauses.append("i.name = %s")
                params.append(instructor_name)
            if year:
                where_clauses.append("d.financial_year = %s")
                params.append(year)
            
            where_sql = " AND ".join(where_clauses)
            
            # Get total count
            count_sql = f"""
                SELECT COUNT(*)
                FROM dw_data_schema.fact_session_event f
                JOIN dw_data_schema.dim_instructor i ON f.instructor_key = i.instructor_key
                JOIN dw_data_schema.dim_date d ON f.date_key = d.date_key
                WHERE {where_sql}
            """
            cur.execute(count_sql, params)
            total_count = cur.fetchone()["count"]

            # This is a fallback/placeholder query using fact_exposure students as a "reach" metric 
            # until actual feedback data is identified.
            sql = f"""
                SELECT 
                    i.name as instructor_name,
                    d.date,
                    a.activity_name,
                    '4.5/5' as rating, -- Placeholder
                    'Positive observation' as comments -- Placeholder
                FROM dw_data_schema.fact_session_event f
                JOIN dw_data_schema.dim_instructor i ON f.instructor_key = i.instructor_key
                JOIN dw_data_schema.dim_date d ON f.date_key = d.date_key
                JOIN dw_data_schema.dim_activity a ON f.activity_key = a.activity_key
                WHERE {where_sql}
                ORDER BY d.date DESC
                LIMIT %s OFFSET %s
            """
            cur.execute(sql, params + [limit, offset])
            rows = cur.fetchall()
            return {
                "table": [{**row, "date": row["date"].strftime("%Y-%m-%d") if row["date"] else None} for row in rows],
                "total_count": total_count
            }
            
