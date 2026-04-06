import logging
from backend.services.query_utils import fetch_all, fetch_one
from backend.config import DATAMART_SCHEMA_NAME

logger = logging.getLogger(__name__)
DW = DATAMART_SCHEMA_NAME


def get_exposure_session_filters():
    try:
        regions = [r["region_name"] for r in fetch_all(
            f"SELECT DISTINCT region_name FROM {DW}.dim_geography WHERE region_name IS NOT NULL ORDER BY region_name"
        )]
        programs = [r["program_name"] for r in fetch_all(
            f"SELECT DISTINCT program_name FROM {DW}.dim_program WHERE program_name IS NOT NULL ORDER BY program_name"
        )]
        years = [r["year_actual"] for r in fetch_all(
            f"SELECT DISTINCT year_actual FROM {DW}.dim_date WHERE year_actual IS NOT NULL ORDER BY year_actual DESC"
        )]
        months = [{"id": r["month_actual"], "name": r["month_name"].strip()} for r in fetch_all(
            f"SELECT DISTINCT month_actual, TO_CHAR(TO_DATE(month_actual::text,'MM'),'Month') AS month_name FROM {DW}.dim_date ORDER BY month_actual"
        )]
        return {"regions": regions, "programs": programs, "years": years, "months": months}
    except Exception as e:
        logger.error(f"exposure session filters error: {e}")
        return {"regions": [], "programs": [], "years": [], "months": []}


def get_exposure_session_data(region=None, program=None, year=None, month=None, limit=15, offset=0):
    try:
        where_clauses = ["TRUE"]
        params = []
        if region:
            where_clauses.append("g.region_name = %s")
            params.append(region)
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

        kpi_row = fetch_one(f"""
            SELECT
                COALESCE(SUM(e.boys_count + e.girls_count), 0) AS total_students,
                COALESCE(SUM(e.boys_count), 0)                 AS total_boys,
                COALESCE(SUM(e.girls_count), 0)                AS total_girls,
                COUNT(DISTINCT f.session_nk_id)                AS total_sessions
            FROM {DW}.fact_session f
            LEFT JOIN {DW}.dim_geography g  ON f.sk_geography_id = g.sk_geography_id
            LEFT JOIN {DW}.dim_program p    ON f.sk_program_id = p.sk_program_id
            LEFT JOIN {DW}.dim_date d       ON f.date_id = d.date_id
            LEFT JOIN {DW}.fact_attendance_exposure e ON f.session_nk_id = e.session_nk_id
            WHERE {where_sql}
        """, params)

        kpis = [
            {"label": "Total Students Exposed", "value": int(kpi_row.get("total_students", 0) or 0), "icon": "fas fa-user-graduate",  "color": "bg-info"},
            {"label": "Total Boys",              "value": int(kpi_row.get("total_boys", 0) or 0),    "icon": "fas fa-male",           "color": "bg-success"},
            {"label": "Total Girls",             "value": int(kpi_row.get("total_girls", 0) or 0),   "icon": "fas fa-female",         "color": "bg-navy-blue"},
            {"label": "Total Sessions",          "value": int(kpi_row.get("total_sessions", 0) or 0),"icon": "fas fa-chalkboard",     "color": "bg-danger"},
        ]

        total_count = fetch_one(f"""
            SELECT COUNT(*) FROM (
                SELECT e.session_nk_id, e.class_name
                FROM {DW}.fact_session f
                LEFT JOIN {DW}.dim_geography g  ON f.sk_geography_id = g.sk_geography_id
                LEFT JOIN {DW}.dim_program p    ON f.sk_program_id = p.sk_program_id
                LEFT JOIN {DW}.dim_date d       ON f.date_id = d.date_id
                JOIN {DW}.fact_attendance_exposure e ON f.session_nk_id = e.session_nk_id
                WHERE {where_sql}
                GROUP BY e.session_nk_id, e.class_name, g.region_name, g.area_name, p.program_name, d.full_date
            ) AS sub
        """, params).get("count", 0)

        table = fetch_all(f"""
            SELECT
                COALESCE(g.region_name, 'Unknown')    AS region_name,
                COALESCE(g.area_name, 'Unknown')      AS area_name,
                COALESCE(p.program_name, 'Unknown')   AS program_name,
                d.full_date                            AS session_date,
                COALESCE(s.school_name, 'Unknown')    AS school_name,
                COALESCE(e.class_name, 'Unknown')     AS class_name,
                COALESCE(SUM(e.boys_count), 0)        AS boys,
                COALESCE(SUM(e.girls_count), 0)       AS girls,
                COALESCE(SUM(e.total_exposure_count), 0) AS total_exposure
            FROM {DW}.fact_session f
            LEFT JOIN {DW}.dim_geography g  ON f.sk_geography_id = g.sk_geography_id
            LEFT JOIN {DW}.dim_program p    ON f.sk_program_id = p.sk_program_id
            LEFT JOIN {DW}.dim_date d       ON f.date_id = d.date_id
            LEFT JOIN {DW}.dim_school s     ON f.sk_school_id = s.sk_school_id
            JOIN {DW}.fact_attendance_exposure e ON f.session_nk_id = e.session_nk_id
            WHERE {where_sql}
            GROUP BY g.region_name, g.area_name, p.program_name, d.full_date, s.school_name, e.class_name
            ORDER BY d.full_date DESC
            LIMIT %s OFFSET %s
        """, params + [limit, offset])

        formatted = []
        for r in table:
            row = dict(r)
            if row.get("session_date"):
                row["session_date"] = row["session_date"].strftime("%Y-%m-%d")
            formatted.append(row)

        return {"kpis": kpis, "table": formatted, "total_count": int(total_count)}
    except Exception as e:
        logger.error(f"exposure session data error: {e}", exc_info=True)
        return {"kpis": [], "table": [], "total_count": 0}
