import logging
from backend.services.query_utils import fetch_all, fetch_one, parse_multi_param, build_multi_value_clause, build_quarter_clause
from backend.config import DATAMART_SCHEMA_NAME

logger = logging.getLogger(__name__)


def get_school_visit_filters():
    try:
        locations = fetch_all(f"""
            SELECT DISTINCT g.region_name, g.area_name
            FROM {DATAMART_SCHEMA_NAME}.dim_geography g
            INNER JOIN {DATAMART_SCHEMA_NAME}.fact_session f ON g.sk_geography_id = f.sk_geography_id
            WHERE g.region_name IS NOT NULL
            ORDER BY g.region_name, g.area_name
        """)

        programs = [row["program_name"] for row in fetch_all(
            f"SELECT DISTINCT program_name FROM {DATAMART_SCHEMA_NAME}.dim_program WHERE program_name IS NOT NULL ORDER BY program_name"
        )]

        years = [row["year_actual"] for row in fetch_all(
            f"SELECT DISTINCT year_actual FROM {DATAMART_SCHEMA_NAME}.dim_date WHERE year_actual IS NOT NULL ORDER BY year_actual DESC"
        )]

        months = [{"id": row["month_actual"], "name": row["month_name"].strip()} for row in fetch_all(
            f"SELECT DISTINCT month_actual, TO_CHAR(TO_DATE(month_actual::text, 'MM'), 'Month') as month_name FROM {DATAMART_SCHEMA_NAME}.dim_date ORDER BY month_actual"
        )]

        quarters = [
            {"id": 1, "name": "Q1 (Apr-Jun)"},
            {"id": 2, "name": "Q2 (Jul-Sep)"},
            {"id": 3, "name": "Q3 (Oct-Dec)"},
            {"id": 4, "name": "Q4 (Jan-Mar)"},
        ]

        return {
            "regions": sorted(list(set(row["region_name"] for row in locations))),
            "areas": sorted(list(set(row["area_name"] for row in locations if row.get("area_name")))),
            "programs": programs,
            "years": years,
            "months": months,
            "quarters": quarters
        }
    except Exception as e:
        logger.error(f"Error fetching school visit filters: {e}", exc_info=True)
        return {"regions": [], "areas": [], "programs": [], "years": [], "months": [], "quarters": []}


def _build_where(region=None, area=None, program=None, year=None, month=None, quarter=None):
    """Build WHERE clauses supporting multi-select CSV values with V4 schema."""
    where_clauses = ["TRUE"]
    params = []

    regions = parse_multi_param(region)
    clause = build_multi_value_clause("g.region_name", regions, params)
    if clause:
        where_clauses.append(clause)

    areas = parse_multi_param(area)
    clause = build_multi_value_clause("g.area_name", areas, params)
    if clause:
        where_clauses.append(clause)

    programs = parse_multi_param(program)
    clause = build_multi_value_clause("p.program_name", programs, params)
    if clause:
        where_clauses.append(clause)

    years = parse_multi_param(year)
    if years:
        int_years = [int(y) for y in years if y.isdigit()]
        clause = build_multi_value_clause("d.year_actual", int_years, params)
        if clause:
            where_clauses.append(clause)

    months = parse_multi_param(month)
    if months:
        int_months = [int(m) for m in months if m.isdigit()]
        clause = build_multi_value_clause("d.month_actual", int_months, params)
        if clause:
            where_clauses.append(clause)

    q_clause = build_quarter_clause(quarter, "d.quarter_actual", params)
    if q_clause:
        where_clauses.append(q_clause)

    return " AND ".join(where_clauses), params


def get_school_visit_data(region=None, area=None, program=None, year=None, month=None, quarter=None, limit=15, offset=0):
    try:
        where_sql, params = _build_where(region, area, program, year, month, quarter)

        # KPIs
        kpi_sql = f"""
            SELECT
                COUNT(DISTINCT f.sk_school_id) as total_schools,
                COUNT(f.sk_fact_session_id) as total_sessions,
                SUM(COALESCE(sess_agg.total_students, 0)) as total_students
            FROM {DATAMART_SCHEMA_NAME}.fact_session f
            LEFT JOIN {DATAMART_SCHEMA_NAME}.dim_geography g ON f.sk_geography_id = g.sk_geography_id
            LEFT JOIN {DATAMART_SCHEMA_NAME}.dim_program p ON f.sk_program_id = p.sk_program_id
            LEFT JOIN {DATAMART_SCHEMA_NAME}.dim_date d ON f.date_id = d.date_id
            LEFT JOIN (
                SELECT session_nk_id, SUM(total_exposure_count) as total_students
                FROM {DATAMART_SCHEMA_NAME}.fact_attendance_exposure
                GROUP BY session_nk_id
            ) sess_agg ON f.session_nk_id = sess_agg.session_nk_id
            WHERE {where_sql}
        """
        kpi_res = fetch_one(kpi_sql, params)

        # Monthly Sessions (latest month in selected period)
        _, params_m = _build_where(region, area, program, year, month, quarter)
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
                LEFT JOIN {DATAMART_SCHEMA_NAME}.dim_date d2 ON f2.date_id = d2.date_id
                LEFT JOIN {DATAMART_SCHEMA_NAME}.dim_geography g2 ON f2.sk_geography_id = g2.sk_geography_id
                LEFT JOIN {DATAMART_SCHEMA_NAME}.dim_program p2 ON f2.sk_program_id = p2.sk_program_id
                WHERE {where_sql}
            )
        """
        # Need to duplicate params for nested subquery WHERE
        _, params_m2 = _build_where(region, area, program, year, month, quarter)
        monthly_res = fetch_one(monthly_sql, params_m + params_m2)

        kpis = {
            "total_schools": int(kpi_res.get("total_schools", 0) or 0),
            "total_students": int(kpi_res.get("total_students", 0) or 0),
            "total_sessions": int(kpi_res.get("total_sessions", 0) or 0),
            "monthly_sessions": int(monthly_res.get("monthly_sessions", 0) or 0)
        }

        # Trend data (monthly sessions for line chart)
        _, params_t = _build_where(region, area, program, year, month, quarter)
        trend_sql = f"""
            SELECT
                d.year_actual, d.month_actual,
                TO_CHAR(TO_DATE(d.month_actual::text, 'MM'), 'Mon') || ' ' || d.year_actual::text as label,
                COUNT(f.sk_fact_session_id) as sessions,
                COUNT(DISTINCT f.sk_school_id) as schools
            FROM {DATAMART_SCHEMA_NAME}.fact_session f
            LEFT JOIN {DATAMART_SCHEMA_NAME}.dim_date d ON f.date_id = d.date_id
            LEFT JOIN {DATAMART_SCHEMA_NAME}.dim_geography g ON f.sk_geography_id = g.sk_geography_id
            LEFT JOIN {DATAMART_SCHEMA_NAME}.dim_program p ON f.sk_program_id = p.sk_program_id
            WHERE {where_sql}
            GROUP BY d.year_actual, d.month_actual
            ORDER BY d.year_actual, d.month_actual
        """
        trends = [{"label": r["label"].strip(), "sessions": int(r["sessions"] or 0), "schools": int(r["schools"] or 0)} for r in fetch_all(trend_sql, params_t)]

        # Pie chart (sessions by program)
        _, params_p = _build_where(region, area, program, year, month, quarter)
        pie_sql = f"""
            SELECT
                p.program_name as label,
                COUNT(f.sk_fact_session_id) as value
            FROM {DATAMART_SCHEMA_NAME}.fact_session f
            LEFT JOIN {DATAMART_SCHEMA_NAME}.dim_program p ON f.sk_program_id = p.sk_program_id
            LEFT JOIN {DATAMART_SCHEMA_NAME}.dim_date d ON f.date_id = d.date_id
            LEFT JOIN {DATAMART_SCHEMA_NAME}.dim_geography g ON f.sk_geography_id = g.sk_geography_id
            WHERE {where_sql} AND p.program_name IS NOT NULL
            GROUP BY p.program_name
            ORDER BY value DESC
            LIMIT 8
        """
        pie_data = [{"label": r["label"], "value": int(r["value"] or 0)} for r in fetch_all(pie_sql, params_p)]

        # Pagination count
        _, params_c = _build_where(region, area, program, year, month, quarter)
        count_sql = f"""
            SELECT COUNT(*) FROM (
                SELECT f.sk_school_id
                FROM {DATAMART_SCHEMA_NAME}.fact_session f
                LEFT JOIN {DATAMART_SCHEMA_NAME}.dim_geography g ON f.sk_geography_id = g.sk_geography_id
                LEFT JOIN {DATAMART_SCHEMA_NAME}.dim_program p ON f.sk_program_id = p.sk_program_id
                LEFT JOIN {DATAMART_SCHEMA_NAME}.dim_date d ON f.date_id = d.date_id
                WHERE {where_sql}
                GROUP BY f.sk_school_id, p.program_name, g.region_name, g.area_name
            ) as sub
        """
        total_count = fetch_one(count_sql, params_c).get("count", 0)

        # Paginated table
        _, params_d = _build_where(region, area, program, year, month, quarter)
        data_sql = f"""
            SELECT
                COALESCE(s.school_name, 'Unknown') as school_name,
                COALESCE(p.program_name, 'Unknown') as program_name,
                COALESCE(g.region_name, 'Unknown') as region,
                COALESCE(g.area_name, 'Unknown') as area,
                COUNT(f.sk_fact_session_id) as sessions,
                SUM(COALESCE(sess_agg.total_students, 0)) as students
            FROM {DATAMART_SCHEMA_NAME}.fact_session f
            LEFT JOIN {DATAMART_SCHEMA_NAME}.dim_geography g ON f.sk_geography_id = g.sk_geography_id
            LEFT JOIN {DATAMART_SCHEMA_NAME}.dim_program p ON f.sk_program_id = p.sk_program_id
            LEFT JOIN {DATAMART_SCHEMA_NAME}.dim_date d ON f.date_id = d.date_id
            LEFT JOIN {DATAMART_SCHEMA_NAME}.dim_school s ON f.sk_school_id = s.sk_school_id
            LEFT JOIN (
                SELECT session_nk_id, SUM(total_exposure_count) as total_students
                FROM {DATAMART_SCHEMA_NAME}.fact_attendance_exposure
                GROUP BY session_nk_id
            ) sess_agg ON f.session_nk_id = sess_agg.session_nk_id
            WHERE {where_sql}
            GROUP BY s.school_name, p.program_name, g.region_name, g.area_name
            ORDER BY sessions DESC
            LIMIT %s OFFSET %s
        """
        table_data = fetch_all(data_sql, params_d + [limit, offset])

        return {
            "table": table_data,
            "total_count": total_count,
            "kpis": kpis,
            "trends": trends,
            "pie": pie_data
        }

    except Exception as e:
        logger.error(f"Error in school visit data: {e}", exc_info=True)
        return {"table": [], "total_count": 0, "kpis": {"total_schools": 0, "total_students": 0, "total_sessions": 0, "monthly_sessions": 0}, "trends": [], "pie": []}
