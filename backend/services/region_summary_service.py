import logging
from backend.services.query_utils import fetch_all, fetch_one
from backend.config import DATAMART_SCHEMA_NAME

logger = logging.getLogger(__name__)


def get_region_summary_filters():
    try:
        # Fetch only programs that have associated data in fact_session
        query = f"""
            SELECT DISTINCT p.program_name 
            FROM {DATAMART_SCHEMA_NAME}.dim_program p
            INNER JOIN {DATAMART_SCHEMA_NAME}.fact_session f ON p.sk_program_id = f.sk_program_id
            WHERE p.program_name IS NOT NULL 
            ORDER BY p.program_name
        """
        programs = [row["program_name"] for row in fetch_all(query)]
        
        years = [row["year_actual"] for row in fetch_all(f"SELECT DISTINCT year_actual FROM {DATAMART_SCHEMA_NAME}.dim_date WHERE year_actual IS NOT NULL ORDER BY year_actual DESC")]
        
        months = [{"id": row["month_actual"], "name": row["month_name"].strip()} for row in fetch_all(f"SELECT DISTINCT month_actual, TO_CHAR(TO_DATE(month_actual::text, 'MM'), 'Month') as month_name FROM {DATAMART_SCHEMA_NAME}.dim_date ORDER BY month_actual")]
        
        return {
            "programs": programs,
            "years": years,
            "months": months
        }
    except Exception as e:
        logger.error(f"Error fetching region summary filters: {e}")
        return {"programs": [], "years": [], "months": []}


def get_region_summary_data(program_type=None, year=None, month=None, limit=15, offset=0):
    try:
        where_clauses = ["TRUE"]
        params = []
        
        # Clean inputs - frontend might send "null", "undefined", or empty strings
        if program_type and str(program_type).strip() not in ["", "null", "undefined", "Select Program"]:
            where_clauses.append("p.program_name = %s")
            params.append(program_type)
        if year and str(year).strip() not in ["", "null", "undefined", "Select Year"]:
            where_clauses.append("d.year_actual = %s")
            params.append(int(year))
        if month and str(month).strip() not in ["", "null", "undefined", "Select Month"]:
            where_clauses.append("d.month_actual = %s")
            params.append(int(month))
        
        where_sql = " AND ".join(where_clauses)
        
        # 1. Get total count of regions with data using LEFT JOIN for maximum coverage
        count_sql = f"""
            SELECT COUNT(DISTINCT COALESCE(g.region_name, 'Unknown')) as count
            FROM {DATAMART_SCHEMA_NAME}.fact_session f
            LEFT JOIN {DATAMART_SCHEMA_NAME}.dim_geography g ON f.sk_geography_id = g.sk_geography_id
            LEFT JOIN {DATAMART_SCHEMA_NAME}.dim_date d ON f.date_id = d.date_id
            LEFT JOIN {DATAMART_SCHEMA_NAME}.dim_program p ON f.sk_program_id = p.sk_program_id
            WHERE {where_sql}
        """
        count_res = fetch_one(count_sql, params)
        total_count = int(count_res.get("count", 0))

        # 2. Main data query with session-level aggregation subquery for exposure
        main_sql = f"""
            SELECT 
                COALESCE(g.region_name, 'Unknown') as region,
                COUNT(f.sk_fact_session_id) as sessions,
                SUM(COALESCE(sess_agg.total_students, 0)) as students_reached,
                SUM(COALESCE(f.no_of_teachers_participated, 0)) as teachers_trained,
                COUNT(DISTINCT f.sk_school_id) as schools_covered
            FROM {DATAMART_SCHEMA_NAME}.fact_session f
            LEFT JOIN {DATAMART_SCHEMA_NAME}.dim_geography g ON f.sk_geography_id = g.sk_geography_id
            LEFT JOIN {DATAMART_SCHEMA_NAME}.dim_date d ON f.date_id = d.date_id
            LEFT JOIN {DATAMART_SCHEMA_NAME}.dim_program p ON f.sk_program_id = p.sk_program_id
            LEFT JOIN (
                SELECT session_nk_id, SUM(total_exposure_count) as total_students
                FROM {DATAMART_SCHEMA_NAME}.fact_attendance_exposure
                GROUP BY session_nk_id
            ) sess_agg ON f.session_nk_id = sess_agg.session_nk_id
            WHERE {where_sql}
            GROUP BY COALESCE(g.region_name, 'Unknown')
            ORDER BY region
            LIMIT %s OFFSET %s
        """
        table_data = fetch_all(main_sql, params + [limit, offset])
        
        # 3. Aggregated KPIs for top cards (Removing CV count)
        kpi_sql = f"""
            SELECT 
                SUM(CASE WHEN (p.program_name ILIKE '%%STEM%%' OR p.program_name ILIKE '%%School%%' OR a.activity_name ILIKE '%%Lab%%') THEN 1 ELSE 0 END) as school_visits,
                SUM(CASE WHEN a.activity_name ILIKE '%%Fair%%' THEN 1 ELSE 0 END) as sf_count,
                SUM(CASE WHEN (p.program_name ILIKE '%%Teacher%%' OR a.activity_name ILIKE '%%Training%%') THEN 1 ELSE 0 END) as ttp_count
            FROM {DATAMART_SCHEMA_NAME}.fact_session f
            LEFT JOIN {DATAMART_SCHEMA_NAME}.dim_geography g ON f.sk_geography_id = g.sk_geography_id
            LEFT JOIN {DATAMART_SCHEMA_NAME}.dim_date d ON f.date_id = d.date_id
            LEFT JOIN {DATAMART_SCHEMA_NAME}.dim_program p ON f.sk_program_id = p.sk_program_id
            LEFT JOIN {DATAMART_SCHEMA_NAME}.dim_activity_type a ON f.sk_activity_type_id = a.sk_activity_type_id
            WHERE {where_sql}
        """
        totals = fetch_one(kpi_sql, params)
        
        return {
            "kpis": [
                {"label": "Total School visits", "value": int(totals.get("school_visits", 0) or 0), "icon": "fas fa-school"},
                {"label": "Total Science Fair (SF)", "value": int(totals.get("sf_count", 0) or 0), "icon": "fas fa-flask"},
                {"label": "Total Teacher Training Program (TTP)", "value": int(totals.get("ttp_count", 0) or 0), "icon": "fas fa-chalkboard-teacher"},
            ],
            "table": table_data,
            "total_count": total_count
        }

    except Exception as e:
        logger.error(f"Error in region summary data: {e}", exc_info=True)
        return {"kpis": [], "table": [], "total_count": 0}




