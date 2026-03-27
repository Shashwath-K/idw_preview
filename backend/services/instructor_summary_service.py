from backend.db import get_datamart_conn

def get_instructor_summary_filters():
    with get_datamart_conn() as conn:
        with conn.cursor() as cur:
            # Regions and Areas
            cur.execute("SELECT DISTINCT state, area FROM dw_data_schema.dim_location WHERE state IS NOT NULL ORDER BY state, area")
            rows = cur.fetchall()
            regions = sorted(list(set(row["state"] for row in rows)))
            areas = sorted(list(set(row["area"] for row in rows)))
            
            # Years (Financial Years)
            cur.execute("SELECT DISTINCT financial_year FROM dw_data_schema.dim_date WHERE financial_year IS NOT NULL ORDER BY financial_year DESC")
            years = [row["financial_year"] for row in cur.fetchall()]
            
            # Months
            cur.execute("SELECT DISTINCT month, TO_CHAR(TO_DATE(month::text, 'MM'), 'Month') as month_name FROM dw_data_schema.dim_date ORDER BY month")
            months = [{"id": row["month"], "name": row["month_name"].strip()} for row in cur.fetchall()]
            
    return {
        "regions": regions,
        "areas": areas,
        "years": years,
        "months": months
    }

def get_instructor_summary_data(region=None, area=None, year=None, month=None, limit=15, offset=0):
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
            
            # Get KPIs
            kpi_sql = f"""
                SELECT 
                    COUNT(DISTINCT d.date) as days_worked,
                    SUM(COALESCE(f.session_count, 0)) as total_sessions,
                    SUM(COALESCE(e.students_total, 0)) as total_exposures,
                    SUM(CASE WHEN a.activity_name ILIKE ANY (ARRAY['%%YIL%%', '%%SF%%', '%%CV%%']) THEN COALESCE(e.students_total, 0) ELSE 0 END) as combined_exposures
                FROM dw_data_schema.dim_instructor i
                LEFT JOIN dw_data_schema.fact_session_event f ON i.instructor_key = f.instructor_key
                LEFT JOIN dw_data_schema.dim_date d ON f.date_key = d.date_key
                LEFT JOIN dw_data_schema.dim_location l ON f.location_key = l.location_key
                LEFT JOIN dw_data_schema.dim_activity a ON f.activity_key = a.activity_key
                LEFT JOIN dw_data_schema.fact_exposure e ON f.session_key = e.session_key
                WHERE {where_sql}
            """
            cur.execute(kpi_sql, params)
            kpi_res = cur.fetchone()
            
            total_exp = int(kpi_res["total_exposures"] or 0)
            comb_exp = int(kpi_res["combined_exposures"] or 0)
            
            kpis = {
                "days_worked": kpi_res["days_worked"] or 0,
                "total_sessions": int(kpi_res["total_sessions"] or 0),
                "school_exposures": total_exp - comb_exp,
                "combined_exposures": comb_exp
            }

            # Get total count for pagination
            count_sql = f"""
                SELECT COUNT(*) FROM (
                    SELECT i.instructor_key
                    FROM dw_data_schema.dim_instructor i
                    LEFT JOIN dw_data_schema.fact_session_event f ON i.instructor_key = f.instructor_key
                    LEFT JOIN dw_data_schema.dim_date d ON f.date_key = d.date_key
                    LEFT JOIN dw_data_schema.dim_location l ON f.location_key = l.location_key
                    WHERE {where_sql}
                    GROUP BY i.instructor_key
                ) as sub
            """
            cur.execute(count_sql, params)
            total_count = cur.fetchone()["count"]

            # Main query to aggregate instructor metrics
            main_sql = f"""
                SELECT 
                    i.name as instructor_name,
                    COUNT(DISTINCT d.date) as days_worked,
                    COUNT(DISTINCT f.session_key) as school_sessions,
                    SUM(COALESCE(f.session_count, 0)) as total_sessions,
                    SUM(COALESCE(e.students_total, 0)) as total_exposures,
                    COUNT(DISTINCT CASE WHEN a.activity_name ILIKE '%%Fair%%' THEN f.session_key END) as fair_count,
                    SUM(CASE WHEN a.activity_name ILIKE '%%Training%%' THEN COALESCE(e.students_total, 0) ELSE 0 END) as training_exposures,
                    SUM(CASE WHEN a.activity_name ILIKE '%%SF%%' THEN COALESCE(e.students_total, 0) ELSE 0 END) as sf_exposures,
                    COUNT(DISTINCT CASE WHEN a.activity_name ILIKE '%%YIL%%' THEN f.session_key END) as yil_sessions,
                    SUM(CASE WHEN a.activity_name ILIKE '%%YIL%%' THEN COALESCE(e.students_total, 0) ELSE 0 END) as yil_exposures,
                    COUNT(DISTINCT CASE WHEN a.activity_name ILIKE '%%CV%%' THEN f.session_key END) as cv_visits,
                    SUM(CASE WHEN a.activity_name ILIKE '%%CV%%' THEN COALESCE(e.students_total, 0) ELSE 0 END) as cv_exposures
                FROM dw_data_schema.dim_instructor i
                LEFT JOIN dw_data_schema.fact_session_event f ON i.instructor_key = f.instructor_key
                LEFT JOIN dw_data_schema.dim_date d ON f.date_key = d.date_key
                LEFT JOIN dw_data_schema.dim_location l ON f.location_key = l.location_key
                LEFT JOIN dw_data_schema.dim_activity a ON f.activity_key = a.activity_key
                LEFT JOIN dw_data_schema.fact_exposure e ON f.session_key = e.session_key
                WHERE {where_sql}
                GROUP BY i.instructor_key, i.name
                ORDER BY i.name
                LIMIT %s OFFSET %s
            """
            cur.execute(main_sql, params + [limit, offset])
            table_data = cur.fetchall()
            
    return {
        "table": table_data,
        "total_count": total_count,
        "kpis": kpis
    }
