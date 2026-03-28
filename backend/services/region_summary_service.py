from backend.db import get_datamart_conn

def get_region_summary_filters():
    with get_datamart_conn() as conn:
        with conn.cursor() as cur:
            # Get Programs
            cur.execute("SELECT DISTINCT program_category FROM dw_data_schema.dim_program WHERE program_category IS NOT NULL ORDER BY program_category")
            programs = [row["program_category"] for row in cur.fetchall()]
            
            # Get Years 
            cur.execute("SELECT DISTINCT year FROM dw_data_schema.dim_date ORDER BY year DESC")
            years = [row["year"] for row in cur.fetchall()]
            
            # Get Months
            cur.execute("SELECT DISTINCT month, TO_CHAR(TO_DATE(month::text, 'MM'), 'Month') as month_name FROM dw_data_schema.dim_date ORDER BY month")
            months = [{"id": row["month"], "name": row["month_name"].strip()} for row in cur.fetchall()]
            
    return {
        "programs": programs,
        "years": years,
        "months": months
    }

def get_region_summary_data(program_type=None, year=None, month=None, limit=15, offset=0):
    with get_datamart_conn() as conn:
        with conn.cursor() as cur:
            where_clauses = ["TRUE"]
            params = []
            
            if program_type:
                where_clauses.append("p.program_category = %s")
                params.append(program_type)
            if year:
                where_clauses.append("d.year = %s")
                params.append(int(year))
            if month:
                where_clauses.append("d.month = %s")
                params.append(int(month))
            
            where_sql = " AND ".join(where_clauses)
            
            # Get total count
            count_sql = f"""
                SELECT COUNT(DISTINCT l.state)
                FROM dw_data_schema.fact_weekly_program_metrics f
                JOIN dw_data_schema.dim_date d ON f.week_key = d.date_key
                JOIN dw_data_schema.dim_location l ON f.location_key = l.location_key
                JOIN dw_data_schema.dim_program p ON f.program_key = p.program_key
                WHERE {where_sql}
            """
            cur.execute(count_sql, params)
            total_count = cur.fetchone()["count"]

            # Use FACT_WEEKLY_PROGRAM_METRICS for accurate aggregation with program filter
            main_sql = f"""
                SELECT 
                    l.state as region,
                    SUM(f.sessions) as sessions,
                    SUM(f.students_reached) as students_reached,
                    SUM(f.teachers_trained) as teachers_trained,
                    SUM(f.schools_covered) as schools_covered,
                    SUM(f.community_reach) as community_reach
                FROM dw_data_schema.fact_weekly_program_metrics f
                JOIN dw_data_schema.dim_date d ON f.week_key = d.date_key
                JOIN dw_data_schema.dim_location l ON f.location_key = l.location_key
                JOIN dw_data_schema.dim_program p ON f.program_key = p.program_key
                WHERE {where_sql}
                GROUP BY l.state
                ORDER BY l.state
                LIMIT %s OFFSET %s
            """
            cur.execute(main_sql, params + [limit, offset])
            table_data = cur.fetchall()
            
            # Aggregated KPIs from table data (Note: for total KPIs, we might need a separate query without limit/offset if we want total totals, but the current UI seems to show totals for the current filters)
            # To show global filtered totals, let's do one more query.
            kpi_sql = f"""
                SELECT 
                    SUM(CASE WHEN (p.program_name ILIKE '%%STEM for Schools%%' OR p.program_name ILIKE '%%School%%') THEN f.session_count ELSE 0 END) as school_visits,
                    SUM(CASE WHEN a.activity_name ILIKE '%%Fair%%' THEN f.session_count ELSE 0 END) as sf_count,
                    SUM(CASE WHEN (p.program_name ILIKE '%%Community%%' OR a.activity_name ILIKE '%%CV%%') THEN f.session_count ELSE 0 END) as cv_count,
                    SUM(CASE WHEN (p.program_name ILIKE '%%Teacher%%' OR a.activity_name ILIKE '%%Training%%') THEN f.session_count ELSE 0 END) as ttp_count
                FROM dw_data_schema.fact_session_event f
                JOIN dw_data_schema.dim_date d ON f.date_key = d.date_key
                JOIN dw_data_schema.dim_location l ON f.location_key = l.location_key
                JOIN dw_data_schema.dim_program p ON f.program_key = p.program_key
                LEFT JOIN dw_data_schema.dim_activity a ON f.activity_key = a.activity_key
                WHERE {where_sql}
            """
            cur.execute(kpi_sql, params)
            totals = cur.fetchone()
            
    return {
        "kpis": [
            {"label": "Total School visits", "value": int(totals["school_visits"] or 0), "icon": "fas fa-school"},
            {"label": "Total Science Fair (SF)", "value": int(totals["sf_count"] or 0), "icon": "fas fa-flask"},
            {"label": "Total Community Visit (CV)", "value": int(totals["cv_count"] or 0), "icon": "fas fa-users"},
            {"label": "Total Teacher Training Program (TTP)", "value": int(totals["ttp_count"] or 0), "icon": "fas fa-chalkboard-teacher"},
        ],
        "table": table_data,
        "total_count": total_count
    }
