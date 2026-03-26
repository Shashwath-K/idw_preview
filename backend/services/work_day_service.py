from backend.db import get_datamart_conn

def get_work_day_filters():
    with get_datamart_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT region, area FROM dw_data_schema.dim_location WHERE region IS NOT NULL ORDER BY region, area")
            locations = cur.fetchall()
            
            cur.execute("SELECT DISTINCT financial_year FROM dw_data_schema.dim_date WHERE financial_year IS NOT NULL ORDER BY financial_year DESC")
            years = [row["financial_year"] for row in cur.fetchall()]
            
            cur.execute("SELECT DISTINCT month, TO_CHAR(TO_DATE(month::text, 'MM'), 'Month') as month_name FROM dw_data_schema.dim_date ORDER BY month")
            months = [{"id": row["month"], "name": row["month_name"].strip()} for row in cur.fetchall()]
            
    return {
        "regions": sorted(list(set(row["region"] for row in locations))),
        "areas": sorted(list(set(row["area"] for row in locations))),
        "years": years,
        "months": months
    }

def get_work_day_data(region=None, area=None, year=None, month=None):
    with get_datamart_conn() as conn:
        with conn.cursor() as cur:
            where_clauses = ["TRUE"]
            params = []
            if region:
                where_clauses.append("l.region = %s")
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
            
            sql = f"""
                SELECT 
                    i.name as instructor_name,
                    l.region,
                    l.area,
                    COUNT(DISTINCT d.date) as days_worked,
                    STRING_AGG(DISTINCT TO_CHAR(d.date, 'DD'), ', ' ORDER BY TO_CHAR(d.date, 'DD')) as dates_active
                FROM dw_data_schema.fact_session_event f
                JOIN dw_data_schema.dim_instructor i ON f.instructor_key = i.instructor_key
                JOIN dw_data_schema.dim_date d ON f.date_key = d.date_key
                JOIN dw_data_schema.dim_location l ON f.location_key = l.location_key
                WHERE {where_sql}
                GROUP BY i.instructor_key, i.name, l.region, l.area
                ORDER BY days_worked DESC, i.name
            """
            cur.execute(sql, params)
            return cur.fetchall()
