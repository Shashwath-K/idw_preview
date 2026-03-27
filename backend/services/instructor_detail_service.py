from backend.db import get_datamart_conn

def get_instructor_detail_filters():
    with get_datamart_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT name FROM dw_data_schema.dim_instructor WHERE name IS NOT NULL ORDER BY name")
            instructors = [row["name"] for row in cur.fetchall()]
            
            cur.execute("SELECT DISTINCT financial_year FROM dw_data_schema.dim_date WHERE financial_year IS NOT NULL ORDER BY financial_year DESC")
            years = [row["financial_year"] for row in cur.fetchall()]
            
            cur.execute("SELECT DISTINCT month, TO_CHAR(TO_DATE(month::text, 'MM'), 'Month') as month_name FROM dw_data_schema.dim_date ORDER BY month")
            months = [{"id": row["month"], "name": row["month_name"].strip()} for row in cur.fetchall()]
            
    return {
        "instructors": instructors,
        "years": years,
        "months": months
    }

def get_instructor_detail_data(instructor_name=None, year=None, month=None):
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
            if month:
                where_clauses.append("d.month = %s")
                params.append(int(month))
            
            where_sql = " AND ".join(where_clauses)
            
            sql = f"""
                SELECT 
                    d.date,
                    l.school_name,
                    a.activity_name,
                    f.session_duration,
                    COALESCE(e.students_total, 0) as students,
                    CASE WHEN f.is_overdue THEN 'Overdue' ELSE 'Completed' END as status
                FROM dw_data_schema.fact_session_event f
                JOIN dw_data_schema.dim_instructor i ON f.instructor_key = i.instructor_key
                JOIN dw_data_schema.dim_date d ON f.date_key = d.date_key
                JOIN dw_data_schema.dim_location l ON f.location_key = l.location_key
                JOIN dw_data_schema.dim_activity a ON f.activity_key = a.activity_key
                LEFT JOIN dw_data_schema.fact_exposure e ON f.session_key = e.session_key
                WHERE {where_sql}
                ORDER BY d.date DESC
                LIMIT 20
            """
            cur.execute(sql, params)
            rows = cur.fetchall()
            return [{**row, "date": row["date"].strftime("%Y-%m-%d") if row["date"] else None} for row in rows]
