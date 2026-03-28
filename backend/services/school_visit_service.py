from backend.db import get_datamart_conn

def get_school_visit_filters():
    with get_datamart_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT state, area FROM dw_data_schema.dim_location WHERE state IS NOT NULL ORDER BY state, area")
            locations = cur.fetchall()
            
            cur.execute("SELECT DISTINCT program_name FROM dw_data_schema.dim_program WHERE program_name IS NOT NULL ORDER BY program_name")
            programs = [row["program_name"] for row in cur.fetchall()]
            
            cur.execute("SELECT DISTINCT financial_year FROM dw_data_schema.dim_date WHERE financial_year IS NOT NULL ORDER BY financial_year DESC")
            years = [row["financial_year"] for row in cur.fetchall()]
            
            cur.execute("SELECT DISTINCT month, TO_CHAR(TO_DATE(month::text, 'MM'), 'Month') as month_name FROM dw_data_schema.dim_date ORDER BY month")
            months = [{"id": row["month"], "name": row["month_name"].strip()} for row in cur.fetchall()]
            
    return {
        "regions": sorted(list(set(row["state"] for row in locations))),
        "areas": sorted(list(set(row["area"] for row in locations))),
        "programs": programs,
        "years": years,
        "months": months
    }

def get_school_visit_data(region=None, area=None, program=None, year=None, month=None, limit=15, offset=0):
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
            if program:
                where_clauses.append("p.program_name = %s")
                params.append(program)
            if year:
                where_clauses.append("d.financial_year = %s")
                params.append(year)
            if month:
                where_clauses.append("d.month = %s")
                params.append(int(month))
            
            where_sql = " AND ".join(where_clauses)
            
            # Get KPIs
            kpi_sql = f"""
                SELECT 
                    COUNT(DISTINCT l.school_name) as total_schools,
                    SUM(f.session_count) as total_sessions,
                    SUM(COALESCE(e.students_total, 0)) as total_students
                FROM dw_data_schema.fact_session_event f
                JOIN dw_data_schema.dim_location l ON f.location_key = l.location_key
                JOIN dw_data_schema.dim_program p ON f.program_key = p.program_key
                JOIN dw_data_schema.dim_date d ON f.date_key = d.date_key
                LEFT JOIN dw_data_schema.fact_exposure e ON f.session_key = e.session_key
                WHERE {where_sql}
            """
            cur.execute(kpi_sql, params)
            kpi_res = cur.fetchone()

            # Monthly Sessions (sessions in the latest month of the selected period)
            monthly_sql = f"""
                SELECT SUM(session_count) as monthly_sessions
                FROM dw_data_schema.fact_session_event f
                JOIN dw_data_schema.dim_date d ON f.date_key = d.date_key
                JOIN dw_data_schema.dim_location l ON f.location_key = l.location_key
                JOIN dw_data_schema.dim_program p ON f.program_key = p.program_key
                WHERE {where_sql}
                AND d.month = (
                    SELECT MAX(d2.month) 
                    FROM dw_data_schema.fact_session_event f2
                    JOIN dw_data_schema.dim_date d2 ON f2.date_key = d2.date_key
                    JOIN dw_data_schema.dim_location l2 ON f2.location_key = l2.location_key
                    JOIN dw_data_schema.dim_program p2 ON f2.program_key = p2.program_key
                    WHERE {where_sql}
                )
            """
            cur.execute(monthly_sql, params + params)
            monthly_res = cur.fetchone()

            kpis = {
                "total_schools": kpi_res["total_schools"] or 0,
                "total_students": int(kpi_res["total_students"] or 0),
                "total_sessions": int(kpi_res["total_sessions"] or 0),
                "monthly_sessions": int(monthly_res["monthly_sessions"] or 0) if monthly_res else 0
            }

            # Get total row count for pagination
            count_sql = f"""
                SELECT COUNT(*) FROM (
                    SELECT l.school_name
                    FROM dw_data_schema.fact_session_event f
                    JOIN dw_data_schema.dim_location l ON f.location_key = l.location_key
                    JOIN dw_data_schema.dim_program p ON f.program_key = p.program_key
                    JOIN dw_data_schema.dim_date d ON f.date_key = d.date_key
                    WHERE {where_sql}
                    GROUP BY l.school_name, p.program_name, l.state, l.area
                ) as sub
            """
            cur.execute(count_sql, params)
            total_count = cur.fetchone()["count"]

            # Get paginated data
            sql = f"""
                SELECT 
                    l.school_name,
                    p.program_name,
                    l.state as region,
                    l.area,
                    SUM(f.session_count) as sessions,
                    SUM(COALESCE(e.students_total, 0)) as students
                FROM dw_data_schema.fact_session_event f
                JOIN dw_data_schema.dim_location l ON f.location_key = l.location_key
                JOIN dw_data_schema.dim_program p ON f.program_key = p.program_key
                JOIN dw_data_schema.dim_date d ON f.date_key = d.date_key
                LEFT JOIN dw_data_schema.fact_exposure e ON f.session_key = e.session_key
                WHERE {where_sql}
                GROUP BY l.school_name, p.program_name, l.state, l.area
                ORDER BY sessions DESC
                LIMIT %s OFFSET %s
            """
            cur.execute(sql, params + [limit, offset])
            return {
                "table": cur.fetchall(), 
                "total_count": total_count,
                "kpis": kpis
            }
