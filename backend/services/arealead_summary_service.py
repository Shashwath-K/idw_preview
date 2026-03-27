from backend.db import get_datamart_conn

def get_arealead_summary_filters():
    with get_datamart_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT state, area FROM dw_data_schema.dim_location WHERE state IS NOT NULL ORDER BY state, area")
            locations = cur.fetchall()
            
            cur.execute("SELECT DISTINCT financial_year FROM dw_data_schema.dim_date WHERE financial_year IS NOT NULL ORDER BY financial_year DESC")
            years = [row["financial_year"] for row in cur.fetchall()]
            
            cur.execute("SELECT DISTINCT month, TO_CHAR(TO_DATE(month::text, 'MM'), 'Month') as month_name FROM dw_data_schema.dim_date ORDER BY month")
            months = [{"id": row["month"], "name": row["month_name"].strip()} for row in cur.fetchall()]
            
    return {
        "regions": sorted(list(set(row["state"] for row in locations))),
        "areas": sorted(list(set(row["area"] for row in locations))),
        "years": years,
        "months": months
    }

def get_arealead_summary_data(region=None, area=None, year=None, month=None, limit=15, offset=0):
    with get_datamart_conn() as conn:
        with conn.cursor() as cur:
            where_clauses = ["TRUE"]
            params = []
            if region:
                where_clauses.append("l.state = %s")
                params.append(region)
            if area:
                where_clauses.append("l.area = %s")
                params.append(area)
            if year:
                where_clauses.append("d.financial_year = %s")
                params.append(year)
            if month:
                where_clauses.append("d.month = %s")
                params.append(int(month))
            
            where_sql = " AND ".join(where_clauses)
            
            # Get total count
            count_sql = f"""
                SELECT COUNT(*) FROM (
                    SELECT l.area, l.state
                    FROM dw_data_schema.fact_session_event f
                    JOIN dw_data_schema.dim_location l ON f.location_key = l.location_key
                    JOIN dw_data_schema.dim_instructor i ON f.instructor_key = i.instructor_key
                    JOIN dw_data_schema.dim_date d ON f.date_key = d.date_key
                    WHERE {where_sql}
                    GROUP BY l.area, l.state
                ) as sub
            """
            cur.execute(count_sql, params)
            total_count = cur.fetchone()["count"]

            # Get paginated data
            sql = f"""
                SELECT 
                    l.area,
                    l.state as region,
                    COUNT(DISTINCT i.instructor_key) as total_instructors,
                    COUNT(DISTINCT f.session_key) as total_sessions,
                    SUM(COALESCE(e.students_total, 0)) as total_students
                FROM dw_data_schema.fact_session_event f
                JOIN dw_data_schema.dim_location l ON f.location_key = l.location_key
                JOIN dw_data_schema.dim_instructor i ON f.instructor_key = i.instructor_key
                JOIN dw_data_schema.dim_date d ON f.date_key = d.date_key
                LEFT JOIN dw_data_schema.fact_exposure e ON f.session_key = e.session_key
                WHERE {where_sql}
                GROUP BY l.area, l.state
                ORDER BY l.state, l.area
                LIMIT %s OFFSET %s
            """
            cur.execute(sql, params + [limit, offset])
            return {"table": cur.fetchall(), "total_count": total_count}
