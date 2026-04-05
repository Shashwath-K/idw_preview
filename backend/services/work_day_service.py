from backend.services.query_utils import fetch_all, fetch_one
from backend.config import DATAMART_SCHEMA_NAME


def get_work_day_filters():
    # Fetch from new dim_geography and dim_date
    locations = fetch_all(f"SELECT DISTINCT region_name, area_name AS area FROM {DATAMART_SCHEMA_NAME}.dim_geography WHERE region_name IS NOT NULL ORDER BY region_name, area_name")
    
    years = [row["year_actual"] for row in fetch_all(f"SELECT DISTINCT year_actual FROM {DATAMART_SCHEMA_NAME}.dim_date WHERE year_actual IS NOT NULL ORDER BY year_actual DESC")]
    
    months = [{"id": row["month_actual"], "name": row["month_name"].strip()} for row in fetch_all(f"SELECT DISTINCT month_actual, TO_CHAR(TO_DATE(month_actual::text, 'MM'), 'Month') as month_name FROM {DATAMART_SCHEMA_NAME}.dim_date ORDER BY month_actual")]
    
    return {
        "regions": sorted(list(set(row["region_name"] for row in locations))),
        "areas": sorted(list(set(row["area"] for row in locations if row.get("area")))),
        "years": years,
        "months": months
    }


def get_work_day_data(region=None, area=None, year=None, month=None, limit=15, offset=0):
    where_clauses = ["TRUE"]
    params = []
    
    if region:
        where_clauses.append("g.region_name = %s")
        params.append(region)
    if area:
        where_clauses.append("g.area_name = %s")
        params.append(area)
    if year:
        where_clauses.append("d.year_actual = %s")
        params.append(int(year))
    if month:
        where_clauses.append("d.month_actual = %s")
        params.append(int(month))
    
    where_sql = " AND ".join(where_clauses)
    
    # KPI Query
    kpi_sql = f"""
        SELECT 
            COUNT(DISTINCT f.sk_user_id) as total_instructors,
            COUNT(DISTINCT CONCAT(f.sk_user_id, '_', f.date_id)) as total_working_days,
            COUNT(DISTINCT f.sk_geography_id) as active_centers
        FROM {DATAMART_SCHEMA_NAME}.fact_session f
        JOIN {DATAMART_SCHEMA_NAME}.dim_date d ON f.date_id = d.date_id
        JOIN {DATAMART_SCHEMA_NAME}.dim_geography g ON f.sk_geography_id = g.sk_geography_id
        WHERE {where_sql}
    """
    kpis_raw = fetch_one(kpi_sql, params)
    
    instructors = kpis_raw.get('total_instructors', 0)
    working_days = kpis_raw.get('total_working_days', 0)
    avg_days = round(working_days / instructors, 2) if instructors > 0 else 0
    active_centers = kpis_raw.get('active_centers', 0)

    kpi_list = [
        {"label": "Total Instructors", "value": instructors, "icon": "fas fa-users", "color": "bg-info"},
        {"label": "Total Working Days", "value": working_days, "icon": "fas fa-calendar-check", "color": "bg-success"},
        {"label": "Avg Days/Instructor", "value": avg_days, "icon": "fas fa-chart-line", "color": "bg-navy-blue"},
        {"label": "Active Centers", "value": active_centers, "icon": "fas fa-map-marker-alt", "color": "bg-danger"}
    ]

    # Get total count for pagination
    count_sql = f"""
        SELECT COUNT(*) FROM (
            SELECT f.sk_user_id
            FROM {DATAMART_SCHEMA_NAME}.fact_session f
            JOIN {DATAMART_SCHEMA_NAME}.dim_user u ON f.sk_user_id = u.sk_user_id
            JOIN {DATAMART_SCHEMA_NAME}.dim_date d ON f.date_id = d.date_id
            JOIN {DATAMART_SCHEMA_NAME}.dim_geography g ON f.sk_geography_id = g.sk_geography_id
            WHERE {where_sql}
            GROUP BY f.sk_user_id, g.sk_geography_id
        ) as sub
    """
    total_count_row = fetch_one(count_sql, params)
    total_count = total_count_row.get("count", 0) if total_count_row else 0

    # Get paginated data
    sql = f"""
        SELECT 
            u.user_name as instructor_name,
            g.region_name as region,
            COALESCE(g.area_name, 'N/A') as area,
            COUNT(DISTINCT d.full_date) as days_worked,
            STRING_AGG(DISTINCT TO_CHAR(d.full_date, 'DD'), ', ' ORDER BY TO_CHAR(d.full_date, 'DD')) as dates_active
        FROM {DATAMART_SCHEMA_NAME}.fact_session f
        JOIN {DATAMART_SCHEMA_NAME}.dim_user u ON f.sk_user_id = u.sk_user_id
        JOIN {DATAMART_SCHEMA_NAME}.dim_date d ON f.date_id = d.date_id
        JOIN {DATAMART_SCHEMA_NAME}.dim_geography g ON f.sk_geography_id = g.sk_geography_id
        WHERE {where_sql}
        GROUP BY u.user_name, g.region_name, g.area_name
        ORDER BY days_worked DESC, u.user_name
        LIMIT %s OFFSET %s
    """
    rows = fetch_all(sql, params + [limit, offset])
    
    return {
        "kpis": kpi_list,
        "table": rows, 
        "total_count": total_count
    }


