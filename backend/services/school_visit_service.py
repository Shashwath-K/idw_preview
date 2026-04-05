from backend.services.query_utils import fetch_all, fetch_one
from backend.config import DATAMART_SCHEMA_NAME


def get_school_visit_filters(region_name: str | None = None):
    # 1. Fetch Regions
    regions = [row["region_name"] for row in fetch_all(f"SELECT DISTINCT region_name FROM {DATAMART_SCHEMA_NAME}.dim_geography WHERE region_name IS NOT NULL ORDER BY region_name")]
    
    # 2. Fetch Areas and Programs based on Region (Dependent Logic)
    areas_query = f"SELECT DISTINCT area_name AS area FROM {DATAMART_SCHEMA_NAME}.dim_geography WHERE area_name IS NOT NULL"
    prog_query = f"""
        SELECT DISTINCT p.program_name 
        FROM {DATAMART_SCHEMA_NAME}.fact_session f
        LEFT JOIN {DATAMART_SCHEMA_NAME}.dim_program p ON p.sk_program_id = f.sk_program_id
        LEFT JOIN {DATAMART_SCHEMA_NAME}.dim_geography g ON g.sk_geography_id = f.sk_geography_id
        WHERE p.program_name IS NOT NULL
    """
    
    params = []
    if region_name:
        areas_query += " AND region_name = %s"
        prog_query += " AND g.region_name = %s"
        params.append(region_name)
    
    areas = [row["area"] for row in fetch_all(areas_query + " ORDER BY area", params)]
    
    # For Programs, if region is selected, only show programs with data. 
    # If not selected, show empty list to signify "inactive" as per main dashboard pattern.
    programs = []
    if region_name:
        programs = [row["program_name"] for row in fetch_all(prog_query + " ORDER BY p.program_name", params)]

    years = [row["year_actual"] for row in fetch_all(f"SELECT DISTINCT year_actual FROM {DATAMART_SCHEMA_NAME}.dim_date WHERE year_actual IS NOT NULL ORDER BY year_actual DESC")]
    
    months = [{"id": row["month_actual"], "name": row["month_name"].strip()} for row in fetch_all(f"SELECT DISTINCT month_actual, TO_CHAR(TO_DATE(month_actual::text, 'MM'), 'Month') as month_name FROM {DATAMART_SCHEMA_NAME}.dim_date ORDER BY month_actual")]
    
    return {
        "regions": regions,
        "areas": areas,
        "programs": programs,
        "years": years,
        "months": months
    }


def get_school_visit_data(region=None, area=None, program=None, year=None, month=None, limit=15, offset=0):
    where_clauses = ["TRUE"]
    params = []
    
    if region:
        where_clauses.append("g.region_name = %s")
        params.append(region)
    if area:
        where_clauses.append("g.area_name = %s")
        params.append(area)
    if program:
        where_clauses.append("p.program_name = %s")
        params.append(program)
    if year:
        where_clauses.append("d.year_actual = %s")
        params.append(int(year))
    if month:
        where_clauses.append("d.month_actual = %s")
        params.append(int(month))
    
    where_sql = " AND ".join(where_clauses)
    
    # Get KPIs (Using LEFT JOINs for robustness)
    kpi_sql = f"""
        SELECT 
            COUNT(DISTINCT f.sk_school_id) as total_schools,
            COUNT(f.sk_fact_session_id) as total_sessions,
            SUM(COALESCE(e.total_exposure_count, 0)) as total_students
        FROM {DATAMART_SCHEMA_NAME}.fact_session f
        LEFT JOIN {DATAMART_SCHEMA_NAME}.dim_geography g ON f.sk_geography_id = g.sk_geography_id
        LEFT JOIN {DATAMART_SCHEMA_NAME}.dim_program p ON f.sk_program_id = p.sk_program_id
        LEFT JOIN {DATAMART_SCHEMA_NAME}.dim_date d ON f.date_id = d.date_id
        LEFT JOIN {DATAMART_SCHEMA_NAME}.fact_attendance_exposure e ON f.session_nk_id = e.session_nk_id
        WHERE {where_sql}
    """
    kpi_res = fetch_one(kpi_sql, params)

    # Monthly Sessions (sessions in the latest month of the selected period)
    monthly_sql = f"""
        SELECT COUNT(f.sk_fact_session_id) as monthly_sessions
        FROM {DATAMART_SCHEMA_NAME}.fact_session f
        LEFT JOIN {DATAMART_SCHEMA_NAME}.dim_date d ON f.date_id = d.date_id
        LEFT JOIN {DATAMART_SCHEMA_NAME}.dim_geography g ON f.sk_geography_id = g.sk_geography_id
        LEFT JOIN {DATAMART_SCHEMA_NAME}.dim_program p ON f.sk_program_id = p.sk_program_id
        WHERE {where_sql}
        AND d.month_actual = (
            SELECT MAX(d2.month_actual) 
            FROM {DATAMART_SCHEMA_NAME}.fact_session f2
            JOIN {DATAMART_SCHEMA_NAME}.dim_date d2 ON f2.date_id = d2.date_id
            JOIN {DATAMART_SCHEMA_NAME}.dim_geography g2 ON f2.sk_geography_id = g2.sk_geography_id
            JOIN {DATAMART_SCHEMA_NAME}.dim_program p2 ON f2.sk_program_id = p2.sk_program_id
            WHERE {where_sql}
        )
    """
    monthly_res = fetch_one(monthly_sql, params + params)

    kpis = {
        "total_schools": kpi_res.get("total_schools") or 0,
        "total_students": int(kpi_res.get("total_students") or 0),
        "total_sessions": int(kpi_res.get("total_sessions") or 0),
        "monthly_sessions": int(monthly_res.get("monthly_sessions") or 0) if monthly_res else 0
    }

    # Get total row count for pagination
    count_sql = f"""
        SELECT COUNT(*) FROM (
            SELECT f.sk_school_id
            FROM {DATAMART_SCHEMA_NAME}.fact_session f
            LEFT JOIN {DATAMART_SCHEMA_NAME}.dim_school s ON f.sk_school_id = s.sk_school_id
            LEFT JOIN {DATAMART_SCHEMA_NAME}.dim_program p ON f.sk_program_id = p.sk_program_id
            LEFT JOIN {DATAMART_SCHEMA_NAME}.dim_geography g ON f.sk_geography_id = g.sk_geography_id
            LEFT JOIN {DATAMART_SCHEMA_NAME}.dim_date d ON f.date_id = d.date_id
            WHERE {where_sql}
            GROUP BY f.sk_school_id, p.program_name, g.region_name, g.area_name
        ) as sub
    """
    total_count = fetch_one(count_sql, params).get("count", 0)

    # Get paginated data
    sql = f"""
        SELECT 
            COALESCE(s.school_name, 'Unknown') as school_name,
            COALESCE(p.program_name, 'Unknown') as program_name,
            COALESCE(g.region_name, 'Unknown') as region,
            COALESCE(g.area_name, 'N/A') as area,
            COUNT(f.sk_fact_session_id) as sessions,
            SUM(COALESCE(e.total_exposure_count, 0)) as students
        FROM {DATAMART_SCHEMA_NAME}.fact_session f
        LEFT JOIN {DATAMART_SCHEMA_NAME}.dim_school s ON f.sk_school_id = s.sk_school_id
        LEFT JOIN {DATAMART_SCHEMA_NAME}.dim_program p ON f.sk_program_id = p.sk_program_id
        LEFT JOIN {DATAMART_SCHEMA_NAME}.dim_geography g ON f.sk_geography_id = g.sk_geography_id
        LEFT JOIN {DATAMART_SCHEMA_NAME}.dim_date d ON f.date_id = d.date_id
        LEFT JOIN {DATAMART_SCHEMA_NAME}.fact_attendance_exposure e ON f.session_nk_id = e.session_nk_id
        WHERE {where_sql}
        GROUP BY s.school_name, p.program_name, g.region_name, g.area_name
        ORDER BY sessions DESC
        LIMIT %s OFFSET %s
    """
    rows = fetch_all(sql, params + [limit, offset])
    
    return {
        "table": rows, 
        "total_count": total_count,
        "kpis": kpis
    }



