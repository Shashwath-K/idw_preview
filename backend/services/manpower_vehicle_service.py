import logging
from backend.services.query_utils import fetch_all, fetch_one
from backend.config import DATAMART_SCHEMA_NAME

logger = logging.getLogger(__name__)
DW = DATAMART_SCHEMA_NAME


def get_manpower_vehicle_filters():
    try:
        regions = [r["region_name"] for r in fetch_all(
            f"SELECT DISTINCT region_name FROM {DW}.dim_geography WHERE region_name IS NOT NULL ORDER BY region_name"
        )]
        years = [r["year_actual"] for r in fetch_all(
            f"SELECT DISTINCT year_actual FROM {DW}.dim_date WHERE year_actual IS NOT NULL ORDER BY year_actual DESC"
        )]
        months = [{"id": r["month_actual"], "name": r["month_name"].strip()} for r in fetch_all(
            f"SELECT DISTINCT month_actual, TO_CHAR(TO_DATE(month_actual::text,'MM'),'Month') AS month_name FROM {DW}.dim_date ORDER BY month_actual"
        )]
        return {"regions": regions, "years": years, "months": months}
    except Exception as e:
        logger.error(f"manpower vehicle filters error: {e}")
        return {"regions": [], "years": [], "months": []}


def get_manpower_vehicle_data(region=None, year=None, month=None, limit=15, offset=0):
    try:
        where_clauses = ["TRUE"]
        params = []
        if region:
            where_clauses.append("g.region_name = %s")
            params.append(region)
        if year:
            where_clauses.append("d.year_actual = %s")
            params.append(int(year))
        if month:
            where_clauses.append("d.month_actual = %s")
            params.append(int(month))
        where_sql = " AND ".join(where_clauses)

        kpi_row = fetch_one(f"""
            SELECT
                COALESCE(SUM(v.distance_travelled), 0)   AS total_kms,
                COALESCE(SUM(v.fuel_cost), 0)            AS total_fuel_cost,
                COALESCE(SUM(v.fuel_quantity), 0)        AS total_fuel_qty,
                COUNT(DISTINCT v.sk_user_id)             AS active_instructors
            FROM {DW}.fact_vehicle_operations v
            LEFT JOIN {DW}.dim_geography g ON v.sk_geography_id = g.sk_geography_id
            LEFT JOIN {DW}.dim_date d       ON v.date_id = d.date_id
            WHERE {where_sql}
        """, params)

        kpis = [
            {"label": "Total KMs Travelled",   "value": int(kpi_row.get("total_kms", 0) or 0),        "icon": "fas fa-road",          "color": "bg-info"},
            {"label": "Total Fuel Cost (₹)",   "value": round(float(kpi_row.get("total_fuel_cost", 0) or 0), 2), "icon": "fas fa-rupee-sign",    "color": "bg-success"},
            {"label": "Total Fuel (L)",         "value": round(float(kpi_row.get("total_fuel_qty", 0) or 0), 1), "icon": "fas fa-gas-pump",      "color": "bg-navy-blue"},
            {"label": "Active Instructors",     "value": int(kpi_row.get("active_instructors", 0) or 0),"icon": "fas fa-users",         "color": "bg-danger"},
        ]

        total_count = fetch_one(f"""
            SELECT COUNT(*) FROM (
                SELECT g.region_name, p.program_name
                FROM {DW}.fact_vehicle_operations v
                LEFT JOIN {DW}.dim_geography g ON v.sk_geography_id = g.sk_geography_id
                LEFT JOIN {DW}.dim_program p    ON v.sk_program_id = p.sk_program_id
                LEFT JOIN {DW}.dim_date d       ON v.date_id = d.date_id
                WHERE {where_sql}
                GROUP BY g.region_name, p.program_name
            ) AS sub
        """, params).get("count", 0)

        table = fetch_all(f"""
            SELECT
                COALESCE(g.region_name, 'Unknown')          AS region_name,
                COALESCE(p.program_name, 'Unknown')         AS program_name,
                COUNT(DISTINCT v.sk_user_id)                AS instructors,
                COALESCE(SUM(v.distance_travelled), 0)      AS total_kms,
                ROUND(COALESCE(SUM(v.fuel_quantity), 0)::numeric, 1) AS total_fuel_l,
                ROUND(COALESCE(SUM(v.fuel_cost), 0)::numeric, 2)     AS total_cost,
                COUNT(CASE WHEN v.was_vehicle_used THEN 1 END)        AS vehicles_used
            FROM {DW}.fact_vehicle_operations v
            LEFT JOIN {DW}.dim_geography g ON v.sk_geography_id = g.sk_geography_id
            LEFT JOIN {DW}.dim_program p    ON v.sk_program_id = p.sk_program_id
            LEFT JOIN {DW}.dim_date d       ON v.date_id = d.date_id
            WHERE {where_sql}
            GROUP BY g.region_name, p.program_name
            ORDER BY total_kms DESC
            LIMIT %s OFFSET %s
        """, params + [limit, offset])

        return {"kpis": kpis, "table": table, "total_count": int(total_count)}
    except Exception as e:
        logger.error(f"manpower vehicle data error: {e}", exc_info=True)
        return {"kpis": [], "table": [], "total_count": 0}
