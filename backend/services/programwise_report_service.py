from backend.db import get_datamart_conn

def get_programwise_report_filters():
    with get_datamart_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT program_category FROM dw_data_schema.dim_program WHERE program_category IS NOT NULL ORDER BY program_category")
            categories = [row["program_category"] for row in cur.fetchall()]
            
            cur.execute("SELECT DISTINCT financial_year FROM dw_data_schema.dim_date WHERE financial_year IS NOT NULL ORDER BY financial_year DESC")
            years = [row["financial_year"] for row in cur.fetchall()]
            
            cur.execute("SELECT DISTINCT month, TO_CHAR(TO_DATE(month::text, 'MM'), 'Month') as month_name FROM dw_data_schema.dim_date ORDER BY month")
            months = [{"id": row["month"], "name": row["month_name"].strip()} for row in cur.fetchall()]
            
    return {
        "categories": categories,
        "years": years,
        "months": months
    }

def get_programwise_report_data(category=None, year=None, month=None):
    with get_datamart_conn() as conn:
        with conn.cursor() as cur:
            where_clauses = ["TRUE"]
            params = []
            if category:
                where_clauses.append("p.program_category = %s")
                params.append(category)
            if year:
                where_clauses.append("d.financial_year = %s")
                params.append(year)
            if month:
                where_clauses.append("d.month = %s")
                params.append(int(month))
            
            where_sql = " AND ".join(where_clauses)
            
            sql = f"""
                SELECT 
                    p.program_name,
                    p.program_category,
                    COUNT(DISTINCT f.session_key) as sessions,
                    SUM(COALESCE(e.students_total, 0)) as students,
                    COUNT(DISTINCT l.location_key) as schools_covered
                FROM dw_data_schema.fact_session_event f
                JOIN dw_data_schema.dim_program p ON f.program_key = p.program_key
                JOIN dw_data_schema.dim_date d ON f.date_key = d.date_key
                JOIN dw_data_schema.dim_location l ON f.location_key = l.location_key
                LEFT JOIN dw_data_schema.fact_exposure e ON f.session_key = e.session_key
                WHERE {where_sql}
                GROUP BY p.program_key, p.program_name, p.program_category
                ORDER BY sessions DESC
                LIMIT 20
            """
            cur.execute(sql, params)
            return cur.fetchall()
