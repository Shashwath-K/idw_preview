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

def get_region_summary_data(program_type=None, year=None, month=None):
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
            
            # Use FACT_WEEKLY_PROGRAM_METRICS for accurate aggregation with program filter
            main_sql = f"""
                SELECT 
                    l.region,
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
                GROUP BY l.region
                ORDER BY l.region
                LIMIT 20
            """
            cur.execute(main_sql, params)
            table_data = cur.fetchall()
            
            # Aggregated KPIs from table data
            total_sessions = sum(row["sessions"] or 0 for row in table_data)
            total_students = sum(row["students_reached"] or 0 for row in table_data)
            total_schools = sum(row["schools_covered"] or 0 for row in table_data)
            total_community = sum(row["community_reach"] or 0 for row in table_data)
            
    return {
        "kpis": [
            {"label": "Total Sessions", "value": int(total_sessions), "icon": "fas fa-video"},
            {"label": "Students Reached", "value": int(total_students), "icon": "fas fa-user-graduate"},
            {"label": "Schools Covered", "value": int(total_schools), "icon": "fas fa-school"},
            {"label": "Community Reach", "value": int(total_community), "icon": "fas fa-users"},
        ],
        "table": table_data,
        "chart": {
            "labels": [row["region"] for row in table_data],
            "datasets": [
                {
                    "label": "Students Reached",
                    "data": [int(row["students_reached"] or 0) for row in table_data],
                    "backgroundColor": "rgba(60,141,188,0.9)"
                }
            ]
        }
    }
