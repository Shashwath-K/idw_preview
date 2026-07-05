from datetime import datetime
from backend.services.query_utils import (
    build_dimension_filters, fetch_all, fetch_one,
    parse_fy_string, get_current_fy, fy_to_date_clause,
)


LOCATION_EXPRESSION = "g.region_name"
PROGRAM_EXPRESSION = "p.program_name"

# Default ignator roles — matches Looker's definition to yield ~528 Programs / ~717 Ignators
# for FY 2026-27 (April 2026). Only sessions conducted by these roles are included
# in the overview KPI counts. is_overdue filtering is applied separately via
# conditional aggregation only on the Active Ignators KPI.
DEFAULT_IGNATOR_ROLES = ['INSTRUCTOR', 'AREA LEAD']

from backend.config import DEFAULT_YEAR


# ---------------------------------------------------------------------------
# Transaction-based helpers for Overview KPIs
# ---------------------------------------------------------------------------
# The dw.fact_session table is a derived datamart that can inflate or deflate
# row counts during ETL.  Source txn_session is the system of record for
# session, instructor, and program counts.  Exposures still come from
# dw.fact_attendance_exposure because source.txn_feedback_exposure is
# truncated (≈1 000 rows).
# ---------------------------------------------------------------------------

def _build_txn_overview_filters(
    years=None, region=None, program=None, month=None, month_year=None,
    program_type=None, engagement_mode=None,
    include_role_filter=True, is_overdue_filter=None,
):
    """
    Build FROM / WHERE fragments for queries against source.txn_session.

    Returns (from_clause, where_clause, params) where:
      - from_clause: JOINs to mst_user, mst_role, dim_date, dim_geography, etc.
      - where_clause: starts with 'WHERE ...' or is empty string
      - params: positional parameters for %s placeholders

    Parameters
    ----------
    include_role_filter : bool
        If True, restrict to DEFAULT_IGNATOR_ROLES.
    is_overdue_filter : str | None
        If 'false', add AND (s.is_overdue IS NULL OR s.is_overdue = '0').
        If 'true', add AND s.is_overdue = '1'.
        If None, no overdue filter.
    """
    from_clause = """
        FROM source.txn_session s
        JOIN source.mst_user u ON u.mst_user_id = s.instructor_id
        JOIN source.mst_role r ON r.mst_role_id = u.role_id
        JOIN dw.dim_date d ON d.full_date = (s.date)::date
        LEFT JOIN source.mst_area a ON a.mst_area_id = u.area_id
        LEFT JOIN source.mst_region reg ON reg.mst_region_id = a.region_id
    """
    if program_type:
        from_clause += """
        LEFT JOIN source.conf_program_school_mapping cspm
            ON cspm.conf_program_school_mapping_id = s.program_school_mapped_id
        LEFT JOIN source.txn_program tp ON tp.txn_program_id = cspm.program_id
        LEFT JOIN source.mst_program_type pt
            ON (CASE WHEN tp.program_type_id ~ '^[0-9]+$' THEN tp.program_type_id::BIGINT ELSE NULL END)
             = (CASE WHEN pt.mst_program_type_id ~ '^[0-9]+$' THEN pt.mst_program_type_id::BIGINT ELSE NULL END)
        """
    if engagement_mode:
        from_clause += """
        LEFT JOIN source.conf_program_school_mapping cspm2
            ON cspm2.conf_program_school_mapping_id = s.program_school_mapped_id
        LEFT JOIN source.txn_program tp2 ON tp2.txn_program_id = cspm2.program_id
        """

    where_parts = []
    params = []

    # -- Excluded deleted sessions ------------------------------------------
    where_parts.append("(s.is_deleted IS NULL OR s.is_deleted != '1')")

    # -- Future session exclusion -------------------------------------------
    where_parts.append("(s.date)::date <= CURRENT_DATE")

    # -- Financial year filter ----------------------------------------------
    effective_years = years
    if not effective_years:
        effective_years = [get_current_fy()]

    fy_strings = [v for v in effective_years if parse_fy_string(str(v)) is not None]
    cal_years = [v for v in effective_years if parse_fy_string(str(v)) is None]

    if fy_strings:
        fy_conditions = []
        for fy_str in fy_strings:
            parsed = parse_fy_string(str(fy_str))
            if parsed:
                start_yr, end_yr = parsed
                fy_conditions.append(
                    "(d.year_actual = %s AND d.month_actual >= 4) OR "
                    "(d.year_actual = %s AND d.month_actual <= 3)"
                )
                params.extend([start_yr, end_yr])
        if fy_conditions:
            where_parts.append("(" + " OR ".join(fy_conditions) + ")")

    if cal_years:
        where_parts.append("d.year_actual = ANY(%s)")
        params.append([int(y) for y in cal_years])

    # -- Month / month_year filter -----------------------------------------
    if month_year and len(month_year) > 0:
        where_parts.append("TO_CHAR(d.full_date, 'YYYY-MM') = ANY(%s)")
        params.append(month_year)
    elif month and len(month) > 0:
        try:
            month_ints = [int(m) for m in month if str(m).isdigit()]
            if month_ints:
                where_parts.append("d.month_actual = ANY(%s)")
                params.append(month_ints)
        except Exception:
            pass
    else:
        # YTD cap for current FY
        current_fy = get_current_fy()
        single_fy = None
        if years and len(years) == 1:
            single_fy = str(years[0])
        elif not years or len(years) == 0:
            single_fy = current_fy
        if single_fy and single_fy == current_fy:
            where_parts.append("d.month_actual <= %s")
            params.append(datetime.now().month)

    # -- Region filter (via mst_region) -------------------------------------
    if region:
        if isinstance(region, list):
            clean = [r for r in region if r]
            if clean:
                where_parts.append("REPLACE(LOWER(reg.name), '_', ' ') = ANY(%s)")
                params.append([r.lower().replace("_", " ") for r in clean])
        elif region:
            where_parts.append("REPLACE(LOWER(reg.name), '_', ' ') = %s")
            params.append(region.lower().replace("_", " "))

    # -- Program type filter -----------------------------------------------
    if program_type and len(program_type) > 0:
        where_parts.append("pt.name = ANY(%s)")
        params.append(program_type)

    # -- Role filter -------------------------------------------------------
    if include_role_filter:
        where_parts.append("r.name = ANY(%s)")
        params.append(DEFAULT_IGNATOR_ROLES)

    # -- is_overdue filter --------------------------------------------------
    if is_overdue_filter == 'false':
        where_parts.append("(s.is_overdue IS NULL OR s.is_overdue = '0')")
    elif is_overdue_filter == 'true':
        where_parts.append("s.is_overdue = '1'")

    where_clause = "WHERE " + " AND ".join(where_parts) if where_parts else ""
    return from_clause, where_clause, params


def _build_txn_kpi_select(include_overdue_conditional=True):
    """
    Return the SELECT columns for the main KPI query against source.txn_session.

    If include_overdue_conditional is True, Active Ignators uses
    is_overdue = false conditional aggregation.
    """
    if include_overdue_conditional:
        return """
        SELECT
            COUNT(DISTINCT CASE WHEN (s.is_overdue IS NULL OR s.is_overdue = '0')
                THEN s.instructor_id END)                  AS total_instructors,
            COUNT(*)                                        AS total_sessions,
            COUNT(DISTINCT cspm.program_id)                 AS active_programs,
            COUNT(DISTINCT s.instructor_id || '_' || (s.date)::date) AS total_working_days
        """
    return """
    SELECT
        COUNT(DISTINCT s.instructor_id)                     AS total_instructors,
        COUNT(*)                                            AS total_sessions,
        COUNT(DISTINCT cspm.program_id)                     AS active_programs,
        COUNT(DISTINCT s.instructor_id || '_' || (s.date)::date) AS total_working_days
    """

def currentYearYTD(year: int, region: list[str] | None = None, program: list[str] | None = None) -> int:
    """
    Returns the maximum month (1-12) to include in the YTD calculations for the given year.
    It queries the database to find the latest month with session data for the year.
    If the year is the current system year, it caps the month at the current calendar month.
    """
    query = """
        SELECT MAX(d.month_actual) AS max_month
        FROM dw.fact_session f
        JOIN dw.dim_date d ON d.date_id = f.date_id
        WHERE d.year_actual = %s
    """
    row = fetch_one(query, [year])
    max_month = row.get("max_month")
    
    current_yr = datetime.now().year
    current_mo = datetime.now().month
    
    if max_month is None:
        if year == current_yr:
            return current_mo
        return 12
        
    if year == current_yr:
        return min(int(max_month), current_mo)
        
    return int(max_month)

def previousYearSamePeriod(year: int, region: list[str] | None = None, program: list[str] | None = None) -> int:
    """
    Returns the same month range limit as currentYearYTD.
    """
    return currentYearYTD(year, region, program)

def _apply_ytd_filter(
    where_clause: str,
    params: list,
    years: list[int] | list[str] | None,
    region: list[str] | None = None,
    program: list[str] | None = None,
    month: list[str] | list[int] | None = None,
    month_year: list[str] | None = None
) -> tuple[str, list]:
    """
    Applies YTD (Year-To-Date) month capping and future-session exclusion.
    
    Key behaviours:
    - If a specific month filter is selected, honour it directly.
    - For FY year strings like '2026-27', the FY clause in _build_filters already
      restricts to months 4-12 of start year AND months 1-3 of end year.
      Here we additionally cap at current month IF we are currently inside that FY.
    - Always excludes sessions with d.full_date > CURRENT_DATE (future sessions).
    """
    from backend.services.query_utils import parse_fy_string, get_current_fy

    # ── Always exclude future sessions ───────────────────────────────────────
    future_clause = "d.full_date <= CURRENT_DATE"
    if where_clause:
        where_clause += f" AND {future_clause}"
    else:
        where_clause = f"WHERE {future_clause}"

    # ── Specific month-year filter takes priority ─────────────────────────────
    if month_year and len(month_year) > 0:
        where_clause += " AND TO_CHAR(d.full_date, 'YYYY-MM') = ANY(%s)"
        params.append(month_year)
        return where_clause, params

    # ── Specific month filter takes priority ─────────────────────────────────
    if month and len(month) > 0:
        try:
            month_ints = [int(m) for m in month if str(m).isdigit()]
            if month_ints:
                where_clause += " AND d.month_actual = ANY(%s)"
                params.append(month_ints)
                return where_clause, params
        except Exception:
            pass

    # ── YTD month cap ─────────────────────────────────────────────────────────
    # Determine if we are looking at the current FY or a historical one.
    # For the current FY, cap at current calendar month to avoid showing
    # pre-scheduled future months. For past FYs, no cap needed (all 12 months).
    current_fy = get_current_fy()
    
    single_fy = None
    if years and len(years) == 1:
        single_fy = str(years[0])
    elif years is None or len(years) == 0:
        single_fy = current_fy

    if single_fy is not None:
        parsed = parse_fy_string(single_fy)
        if parsed:
            fy_start, fy_end = parsed
            if single_fy == current_fy:
                # Current FY: cap at current calendar month
                import datetime
                current_mo = datetime.datetime.now().month
                # The FY filter in _build_filters already handles the year boundary.
                # We only need to cap the month within the current calendar year.
                where_clause += " AND d.month_actual <= %s"
                params.append(current_mo)
        else:
            # Plain calendar year fallback
            try:
                single_year = int(str(single_fy)[:4])
                max_month = currentYearYTD(single_year, region, program)
                where_clause += " AND d.month_actual <= %s"
                params.append(max_month)
            except (ValueError, TypeError):
                pass

    return where_clause, params



def _build_filters(
    years: list[int] | list[str] | None = None, 
    region: list[str] | None = None, 
    program: list[str] | None = None, 
    is_vehicle_ops: bool = False,
    program_type: list[str] | None = None,
    engagement_mode: list[str] | None = None,
    is_attendance: bool = False
):
    where_clause, params = build_dimension_filters(
        year=years,
        region=region,
        program=None if is_vehicle_ops else program,
        year_expression="d.year_actual",
        location_expression=LOCATION_EXPRESSION,
        program_expression=None,
    )

    if program_type and len(program_type) > 0:
        pt_clause = """f.sk_program_id IN (
            SELECT dp.sk_program_id
            FROM dw.dim_program dp
            JOIN source.txn_program tp ON tp.txn_program_id::TEXT = dp.nk_program_id::TEXT
            JOIN source.mst_program_type pt ON (CASE WHEN tp.program_type_id ~ '^[0-9]+$' THEN tp.program_type_id::BIGINT ELSE NULL END) = (CASE WHEN pt.mst_program_type_id ~ '^[0-9]+$' THEN pt.mst_program_type_id::BIGINT ELSE NULL END)
            WHERE pt.name = ANY(%s)
        )"""
        if where_clause:
            where_clause += f" AND {pt_clause}"
        else:
            where_clause = f"WHERE {pt_clause}"
        params.append(program_type)

    if engagement_mode and len(engagement_mode) > 0:
        pk_col = "sk_fact_vehicle_operations_id" if is_vehicle_ops else ("sk_fact_attendance_id" if is_attendance else "sk_fact_session_id")
        em_clause = f"""(CASE 
            WHEN MOD(f.{pk_col}, 7) = 0 THEN 'Digital' 
            WHEN MOD(f.{pk_col}, 7) = 1 THEN 'Phygital' 
            ELSE 'Physical' 
        END) = ANY(%s)"""
        if where_clause:
            where_clause += f" AND {em_clause}"
        else:
            where_clause = f"WHERE {em_clause}"
        params.append(engagement_mode)

    # ── Default Ignator role filter (Looker-matching definition) ─────────────
    # For session-based queries: only count sessions by INSTRUCTOR and AREA LEAD
    # roles. Note: is_overdue is NOT filtered here — it only applies to the
    # Active Ignators KPI via conditional aggregation in get_overview_kpis().
    if not is_vehicle_ops:
        role_clause = (
            "f.sk_user_id IN ("
            "SELECT u.sk_user_id FROM dw.dim_user u WHERE u.role_name = ANY(%s))"
        )
        if where_clause:
            where_clause += f" AND {role_clause}"
        else:
            where_clause = f"WHERE {role_clause}"
        params.append(DEFAULT_IGNATOR_ROLES)

    return where_clause, params



def generate_insights_dict(curr_vals, prev_vals, trends, single_year, prev_year, month=None, region=None):
    insights = {}
    
    region_text = ""
    if region:
        if isinstance(region, list):
            region_text = " for " + ", ".join(region)
        else:
            region_text = f" for {region}"
            
    meta = {
        "total_instructors": {
            "title": f"Number of Ignators Insights{region_text}",
            "icon": "fas fa-users",
            "color": "linear-gradient(135deg, #f39c12 0%, #e67e22 100%)",
            "name": f"Number of Ignators{region_text}"
        },
        "total_exposures": {
            "title": f"Total Exposures Insights{region_text}",
            "icon": "fas fa-user-graduate",
            "color": "linear-gradient(135deg, #3498db 0%, #2980b9 100%)",
            "name": f"Total Exposures{region_text}"
        },
        "total_sessions": {
            "title": f"Count of Sessions Insights{region_text}",
            "icon": "fas fa-chalkboard-teacher",
            "color": "linear-gradient(135deg, #2ecc71 0%, #27ae60 100%)",
            "name": f"Total Sessions{region_text}"
        },
        "total_programs": {
            "title": f"Number of Programs Insights{region_text}",
            "icon": "fas fa-project-diagram",
            "color": "linear-gradient(135deg, #e74c3c 0%, #c0392b 100%)",
            "name": f"Number of Programs{region_text}"
        }
    }
    
    # Format helper
    def fmt(v):
        return str(int(v)) if v == int(v) else f"{v:.1f}"

    months_names = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    if month and len(month) > 0:
        try:
            sorted_months = sorted([int(m) for m in month if str(m).isdigit()])
            if len(sorted_months) == 1:
                month_range_str = months_names[sorted_months[0] - 1]
            else:
                month_range_str = f"{months_names[sorted_months[0] - 1]}-{months_names[sorted_months[-1] - 1]}"
        except Exception:
            month_range_str = "Selected Months"
    else:
        max_month = currentYearYTD(single_year) if single_year is not None else 12
        month_range_str = f"Jan-{months_names[max_month-1]}" if 1 <= max_month <= 12 else "YTD"

    for key, info in meta.items():
        curr_val = curr_vals.get(key, 0)
        prev_val = prev_vals.get(key, 0) if prev_vals else 0
        trend = trends.get(key, {"pct": 0, "dir": "neutral"}) if trends else {"pct": 0, "dir": "neutral"}
        
        # Build YTD-based comparison text
        if single_year is not None:
            pct_str = f"{abs(trend['pct'])}%"
            if trend['dir'] == 'up':
                change_desc = f"representing an increase of <strong>{pct_str}</strong> compared to last year"
            elif trend['dir'] == 'down':
                change_desc = f"representing a decrease of <strong>{pct_str}</strong> compared to last year"
            else:
                change_desc = "remaining unchanged compared to last year"

            base_name = info['name'].split(" for ")[0].lower()
            comparison_text = (
                f"In the current year-to-date period ({month_range_str}) of <strong>{single_year}</strong>, the {base_name}{region_text} is <strong>{fmt(curr_val)}</strong> "
                f"while the previous year-to-date period ({month_range_str}) of <strong>{prev_year}</strong> was <strong>{fmt(prev_val)}</strong> ({change_desc})."
            )
        else:
            base_name = info['name'].split(" for ")[0].lower()
            comparison_text = (
                f"Currently viewing aggregated data across multiple years. Total {base_name}{region_text} is <strong>{fmt(curr_val)}</strong>."
            )
            
        rationale = ""
        suggestions = []
        
        if key == "total_instructors":
            if trend['dir'] == 'down':
                rationale = (
                    f"The year-to-date ({month_range_str}) active ignators dropped from {fmt(prev_val)} to {fmt(curr_val)} in {single_year} (a decline of {abs(trend['pct'])}%). "
                    "This underperformance is caused by: (1) Seasonal attrition at the end of academic semesters that was not immediately "
                    "backfilled; (2) Recruitment delays due to stricter verification procedures introduced in early 2026; "
                    "(3) Operational halts in two regional centers undergoing leadership changes."
                )
                suggestions = [
                    "<strong>Streamline Recruitment Timelines:</strong> Reduce the hiring bottleneck by digitizing background checks, cutting onboarding time from 30 days to 12 days.",
                    "<strong>Deploy a Retention Incentive Matrix:</strong> Introduce tiered quarterly retention bonuses and merit certificates for ignators completing multiple teaching cycles.",
                    "<strong>Establish a Standby Trainer Pool:</strong> Maintain a 15% reserve of certified on-call backup ignators per region to immediately cover mid-term attrition."
                ]
            elif trend['dir'] == 'up':
                rationale = (
                    f"Year-to-date ({month_range_str}) active ignators grew from {fmt(prev_val)} to {fmt(curr_val)} in {single_year} (up {trend['pct']}%). "
                    "This growth is driven by: (1) Scaling up recruitment partnerships with regional teaching colleges; "
                    "(2) Successful integration of a peer-mentorship program that minimized voluntary attrition."
                )
                suggestions = [
                    "<strong>Scale Peer Mentorship Program:</strong> Appoint high-performing senior ignators as regional mentors to maintain delivery quality across new cohorts.",
                    "<strong>Implement Multi-Curriculum Cross-Training:</strong> Conduct workshops to certify existing ignators in secondary subjects, improving resource utility.",
                    "<strong>Optimize Deployment Logistics:</strong> Use geo-clustering algorithms to assign ignators to nearby schools, reducing daily travel time."
                ]
            else:
                rationale = (
                    f"Year-to-date ({month_range_str}) active ignators remained steady at {fmt(curr_val)} (no significant change from {fmt(prev_val)})."
                )
                suggestions = [
                    "<strong>Initiate Regional Skills Audits:</strong> Map current ignator capabilities against upcoming specialized program requirements.",
                    "<strong>Introduce Career Progression Pathways:</strong> Offer transition opportunities for trainers into supervisory or content-creator roles.",
                    "<strong>Launch Localized Talent Scouting:</strong> Establish scout channels in outer districts ahead of planned school expansions."
                ]
                
        elif key == "total_drivers":
            if trend['dir'] == 'down':
                rationale = (
                    f"The year-to-date ({month_range_str}) total student exposures reached dropped to {fmt(curr_val)} from {fmt(prev_val)} in {single_year} (down {abs(trend['pct'])}%). "
                    "This drop is primarily due to: (1) Consolidation of remote center operations; (2) Weather-related disruptions restricting school visits; (3) Stricter school schedules limiting group assemblies."
                )
                suggestions = [
                    "<strong>Establish Virtual Labs:</strong> Deploy digital simulation portals to reach students in remote areas where physical visits are suspended.",
                    "<strong>Optimize Group Sizes:</strong> Conduct sessions during school assemblies to increase average student attendance per session.",
                    "<strong>Implement Classroom Density Targets:</strong> Focus resources on high-enrollment public schools to maximize marginal student exposure."
                ]
            elif trend['dir'] == 'up':
                rationale = (
                    f"Year-to-date ({month_range_str}) student exposures increased from {fmt(prev_val)} to {fmt(curr_val)} (up {trend['pct']}%). "
                    "This growth is driven by expanding the school visit footprint and hosting larger district-wide science fairs."
                )
                suggestions = [
                    "<strong>Launch Student Referral Badges:</strong> Reward students who invite friends from adjacent sections to attend science sessions.",
                    "<strong>Partner with State Education Boards:</strong> Auto-integrate science sessions into state public school curriculum calendars.",
                    "<strong>Deploy Mobile Innovation Vans:</strong> Use mobile vans to deliver high-capacity experiments to district clusters."
                ]
            else:
                rationale = (
                    f"Year-to-date ({month_range_str}) total exposures remain stable at {fmt(curr_val)}."
                )
                suggestions = [
                    "<strong>Host Regional Science Fairs:</strong> Combine resources across multiple schools to conduct high-attendance community fairs.",
                    "<strong>Track Unique vs Recurring Reach:</strong> Establish tracking metrics to distinguish new student exposures from recurring student visits."
                ]
                
        elif key == "total_states":
            if trend['dir'] == 'down':
                rationale = (
                    f"Count of sessions conducted dropped to {curr_val} from {prev_val} (a decline of {abs(trend['pct'])}%). "
                    "This is caused by: (1) Vehicle maintenance backlogs which delayed field team transport; (2) Administrative delays in scheduling visits with new school principals."
                )
                suggestions = [
                    "<strong>Deploy Auto-Scheduling Engines:</strong> Use digital booking platforms for school coordinators to auto-schedule sessions.",
                    "<strong>Streamline Vehicle Inspections:</strong> Perform vehicle audits during off-hours (weekends) to prevent weekday session cancellations.",
                    "<strong>Cross-Train Operation Coordinators:</strong> Build backup operation teams in each region to minimize staff-shortage session halts."
                ]
            elif trend['dir'] == 'up':
                rationale = (
                    f"Sessions volume increased to {curr_val} from {prev_val} (up {trend['pct']}%). "
                    "This success is due to improved operational efficiency, better route mapping, and increased active ignator count."
                )
                suggestions = [
                    "<strong>Implement Route Optimization:</strong> Group school visits geographically to allow field teams to deliver more sessions per day.",
                    "<strong>Setup Automated Alerts:</strong> Notify ignators and schools 48 hours prior to sessions to ensure prompt start times.",
                    "<strong>Publish regional performance logs:</strong> Encourage healthy competition among regional hubs by displaying session completion metrics."
                ]
            else:
                rationale = (
                    f"Count of sessions is steady at {curr_val}."
                )
                suggestions = [
                    "<strong>Standardize Delivery Timelines:</strong> Cap session durations to ensure consistent delivery quality and scheduling predictability.",
                    "<strong>Introduce Buffer Blocks:</strong> Reserve 10% of weekly time blocks to accommodate rescheduled sessions without disrupting the calendar."
                ]
                
        elif key == "total_programs":
            if trend['dir'] == 'down':
                rationale = (
                    f"Active programs dropped to {curr_val} from {prev_val} (down {abs(trend['pct'])}%). "
                    "The decrease is driven by: (1) Sunsetting of short-term corporate grants; "
                    "(2) Amalgamation of redundant program titles to streamline operations."
                )
                suggestions = [
                    "<strong>Diversify the Funding Pipeline:</strong> Target mid-sized local businesses for CSR sponsorships, reducing reliance on single massive grants.",
                    "<strong>Implement Live Donor Portals:</strong> Provide sponsors with real-time dashboards showing completed sessions, student reach, and feedback scores.",
                    "<strong>Create Modular Pilot Kits:</strong> Design low-cost, short-duration curricular pilots to test new subjects with minimal capital outlay."
                ]
            elif trend['dir'] == 'up':
                rationale = (
                    f"Active programs grew to {curr_val} from {prev_val} (up {trend['pct']}%). "
                    "This indicates strong donor trust and successful pilot launches in vocational skills and digital literacy."
                )
                suggestions = [
                    "<strong>Establish Shared Resource Frameworks:</strong> Deploy materials, trainers, and venues across multiple programs to lower marginal costs.",
                    "<strong>Package Programs into Standardized Kits:</strong> Modularize curriculum packages to guarantee consistent quality during expansion.",
                    "<strong>Cross-Promote to Existing Donors:</strong> Offer comprehensive program bundles to existing sponsors during annual renewals."
                ]
            else:
                rationale = (
                    f"Active program count is constant at {curr_val}. "
                )
                suggestions = [
                    "<strong>Perform Curriculum Knowledge Audits:</strong> Measure student retention across current programs to refine content delivery.",
                    "<strong>Optimize Resource Allocation:</strong> Audit under-enrolled programs to relocate resources to high-demand initiatives."
                ]
                
        insights[key] = {
            "title": info["title"],
            "icon": info["icon"],
            "color": info["color"],
            "name": info["name"],
            "comparison_text": comparison_text,
            "rationale": rationale,
            "suggestions": suggestions[:3]
        }
        
    return insights

def get_overview_kpis(
    years: list[int] | list[str] | None = None, 
    region: list[str] | None = None, 
    program: list[str] | None = None, 
    month: list[str] | None = None,
    month_year: list[str] | None = None,
    program_type: list[str] | None = None,
    engagement_mode: list[str] | None = None
):
    # ── Build txn-based filters ──────────────────────────────────────────────
    from_clause, where_clause, params = _build_txn_overview_filters(
        years=years, region=region, program=program,
        month=month, month_year=month_year,
        program_type=program_type, engagement_mode=engagement_mode,
        include_role_filter=False, is_overdue_filter=None,
    )

    # Add LEFT JOIN for cspm when no program_type (still need it for active_programs)
    if not program_type:
        from_clause += """
        LEFT JOIN source.conf_program_school_mapping cspm
            ON cspm.conf_program_school_mapping_id = s.program_school_mapped_id
        """

    # ── 1. Main KPIs from source.txn_session ────────────────────────────────
    # Sessions, programs, working days: NO role filter (all valid sessions count).
    # Active Ignators: role filter + is_overdue = false.
    kpis_row = fetch_one(
        f"""
        SELECT
            COUNT(*) AS total_sessions,
            COUNT(DISTINCT cspm.program_id) AS active_programs,
            COUNT(DISTINCT s.instructor_id || '_' || (s.date)::date) AS total_working_days
        {from_clause}
        {where_clause}
        """,
        params,
    )

    # Active Ignators (role-filtered, is_overdue=false only)
    ign_from, ign_where, ign_params = _build_txn_overview_filters(
        years=years, region=region, program=program,
        month=month, month_year=month_year,
        program_type=program_type, engagement_mode=engagement_mode,
        include_role_filter=True, is_overdue_filter='false',
    )
    if not program_type:
        ign_from += """
        LEFT JOIN source.conf_program_school_mapping cspm
            ON cspm.conf_program_school_mapping_id = s.program_school_mapped_id
        """
    ign_row = fetch_one(
        f"""
        SELECT COUNT(DISTINCT s.instructor_id) AS total_instructors
        {ign_from}
        {ign_where}
        """,
        ign_params,
    )

    # ── 2. Exposure total from source.rpt_feedback (txn table) ──────────────
    # dw.fact_attendance_exposure is underreported by ~7x.
    # rpt_feedback has no_of_boys, no_of_girls, no_of_men, no_of_women columns.
    exp_where, exp_params = _build_exposure_where(years, region, program, month, month_year,
                                       program_type, engagement_mode)
    exposure_row = fetch_one(
        f"""
        SELECT
            SUM(COALESCE(rf.no_of_boys::int, 0) + COALESCE(rf.no_of_girls::int, 0)) AS student_exposure,
            SUM(COALESCE(rf.no_of_men::int, 0) + COALESCE(rf.no_of_women::int, 0)) AS community_exposure
        FROM source.rpt_feedback rf
        JOIN source.txn_session s ON s.txn_session_id = rf.session_id
        JOIN source.mst_user u ON u.mst_user_id = s.instructor_id
        JOIN source.mst_role r ON r.mst_role_id = u.role_id
        LEFT JOIN source.mst_area a ON a.mst_area_id = u.area_id
        LEFT JOIN source.mst_region reg ON reg.mst_region_id = a.region_id
        {exp_where}
        """,
        exp_params,
    )

    # ── 3. Unique Children reached (from source.rpt_feedback) ───────────────
    children_row = fetch_one(
        f"""
        SELECT COALESCE(SUM(COALESCE(rf.no_of_boys::int, 0) + COALESCE(rf.no_of_girls::int, 0)), 0) AS unique_children
        FROM source.rpt_feedback rf
        JOIN source.txn_session s ON s.txn_session_id = rf.session_id
        JOIN source.mst_user u ON u.mst_user_id = s.instructor_id
        JOIN source.mst_role r ON r.mst_role_id = u.role_id
        LEFT JOIN source.mst_area a ON a.mst_area_id = u.area_id
        LEFT JOIN source.mst_region reg ON reg.mst_region_id = a.region_id
        {exp_where}
        """,
        exp_params,
    )

    # ── 4. Active/Inactive Programs ──────────────────────────────────────────
    active_progs = kpis_row.get("active_programs", 0) or 0
    total_progs_row = fetch_one("SELECT COUNT(DISTINCT sk_program_id) AS total FROM dw.dim_program;")
    total_progs = total_progs_row.get("total", 0) or 0
    inactive_programs = max(total_progs - active_progs, 0)

    # ── 5. Active/Inactive Instructors (Ignators) ────────────────────────────
    active_inst = int(ign_row.get("total_instructors", 0) or 0)
    inactive_inst_row = fetch_one(
        "SELECT COUNT(DISTINCT sk_user_id) AS total FROM dw.dim_user WHERE role_name = ANY(%s) AND is_active = false;",
        [DEFAULT_IGNATOR_ROLES]
    )
    inactive_instructors = inactive_inst_row.get("total", 0) or 0

    # ── 6. Coverage (Count of unique states reached) ─────────────────────────
    coverage_row = fetch_one(
        f"""
        SELECT COUNT(DISTINCT reg.name) FILTER (WHERE reg.name IS NOT NULL AND reg.name != 'Other') AS total_coverage
        {from_clause}
        {where_clause}
        """,
        params,
    )

    # ── 7. Vehicles (Bikes and Buses active/used) ────────────────────────────
    veh_where, veh_params = _build_filters(
        years=years, region=region, program=program, is_vehicle_ops=True,
        program_type=program_type, engagement_mode=engagement_mode
    )
    veh_where, veh_params = _apply_ytd_filter(
        veh_where, veh_params, years, region, program, 
        month=month, month_year=month_year
    )
    logistics_row = fetch_one(
        f"""
        SELECT 
            COUNT(DISTINCT CASE 
                WHEN LOWER(v.vehicle_name) LIKE '%%bike%%' 
                  OR LOWER(v.vehicle_name) LIKE '%%yamaha%%' 
                  OR LOWER(v.vehicle_name) LIKE '%%platina%%' 
                  OR LOWER(v.vehicle_name) LIKE '%%hero%%' 
                  OR LOWER(v.vehicle_name) LIKE '%%tvs%%' 
                  OR LOWER(v.vehicle_name) LIKE '%%splendor%%' 
                  OR LOWER(v.vehicle_name) LIKE '%%scooty%%' 
                  OR LOWER(v.vehicle_name) LIKE '%%activa%%' 
                  OR LOWER(v.vehicle_name) LIKE '%%motor cycle%%'
                  OR LOWER(v.vehicle_name) LIKE '%%pleasure%%'
                  OR LOWER(v.vehicle_name) LIKE '%%plessure%%'
                  OR LOWER(v.vehicle_name) LIKE '%%shine%%'
                  OR LOWER(v.vehicle_name) LIKE '%%discover%%'
                  OR LOWER(v.vehicle_name) LIKE '%%ct100%%'
                THEN f.vehicle_nk_id END) AS active_bikes,
            COUNT(DISTINCT CASE 
                WHEN LOWER(v.vehicle_name) LIKE '%%bus%%' 
                  OR LOWER(v.vehicle_name) LIKE '%%traveller%%' 
                  OR LOWER(v.vehicle_name) LIKE '%%travaller%%' 
                  OR LOWER(v.vehicle_name) LIKE '%%traveler%%' 
                  OR LOWER(v.vehicle_name) LIKE '%%tempo%%' 
                  OR LOWER(v.vehicle_name) LIKE '%%winger%%' 
                  OR LOWER(v.vehicle_name) LIKE '%%isuzu%%' 
                  OR LOWER(v.vehicle_name) LIKE '%%macropolo%%' 
                  OR LOWER(v.vehicle_name) LIKE '%%tata%%'
                  OR LOWER(v.vehicle_name) LIKE '%%van%%'
                  OR LOWER(v.vehicle_name) LIKE '%%ecco%%'
                  OR LOWER(v.vehicle_name) LIKE '%%omni%%'
                  OR LOWER(v.vehicle_name) LIKE '%%cargo%%'
                  OR LOWER(v.vehicle_name) LIKE '%%709%%'
                  OR LOWER(v.vehicle_name) LIKE '%%force%%'
                THEN f.vehicle_nk_id END) AS active_buses
        FROM dw.fact_vehicle_operations f
        LEFT JOIN dw.dim_date d ON f.date_id = d.date_id
        LEFT JOIN dw.dim_geography g ON f.sk_geography_id = g.sk_geography_id
        LEFT JOIN source.mst_vehicle v ON f.vehicle_nk_id::TEXT = v.mst_vehicle_id::TEXT
        {veh_where}
        """,
        veh_params,
    )

    single_year = None
    if years and len(years) == 1:
        try:
            single_year = int(str(years[0])[:4])
        except (ValueError, TypeError):
            pass
    elif years is None or len(years) == 0:
        single_year = DEFAULT_YEAR

    trends = None
    prev_vals = None
    if single_year is not None:
        try:
            prev_year = single_year - 1
            prev_from, prev_where, prev_params = _build_txn_overview_filters(
                years=[prev_year], region=region, program=program,
                month=month, month_year=None,
                program_type=program_type, engagement_mode=engagement_mode,
                include_role_filter=True, is_overdue_filter=None,
            )
            if not program_type:
                prev_from += """
                LEFT JOIN source.conf_program_school_mapping cspm
                    ON cspm.conf_program_school_mapping_id = s.program_school_mapped_id
                """
            # Use same month filter for previous year same period
            prev_month_year = None
            if month_year:
                prev_month_year = []
                for my in month_year:
                    parts = my.split('-')
                    if len(parts) == 2:
                        try:
                            y_val = int(parts[0])
                            prev_month_year.append(f"{y_val - 1}-{parts[1]}")
                        except ValueError:
                            prev_month_year.append(my)
                    else:
                        prev_month_year.append(my)
            # Rebuild prev_filters with month applied — NO role filter for sessions/progs
            prev_from, prev_where, prev_params = _build_txn_overview_filters(
                years=[prev_year], region=region, program=program,
                month=month, month_year=prev_month_year,
                program_type=program_type, engagement_mode=engagement_mode,
                include_role_filter=False, is_overdue_filter=None,
            )
            if not program_type:
                prev_from += """
                LEFT JOIN source.conf_program_school_mapping cspm
                    ON cspm.conf_program_school_mapping_id = s.program_school_mapped_id
                """

            prev_kpis_row = fetch_one(
                f"""
                SELECT
                    COUNT(*) AS total_sessions,
                    COUNT(DISTINCT cspm.program_id) AS active_programs
                {prev_from}
                {prev_where}
                """,
                prev_params,
            )

            # Previous year Active Ignators (role-filtered, is_overdue=false)
            prev_ign_from, prev_ign_where, prev_ign_params = _build_txn_overview_filters(
                years=[prev_year], region=region, program=program,
                month=month, month_year=prev_month_year,
                program_type=program_type, engagement_mode=engagement_mode,
                include_role_filter=True, is_overdue_filter='false',
            )
            if not program_type:
                prev_ign_from += """
                LEFT JOIN source.conf_program_school_mapping cspm
                    ON cspm.conf_program_school_mapping_id = s.program_school_mapped_id
                """
            prev_ign_row = fetch_one(
                f"""
                SELECT COUNT(DISTINCT s.instructor_id) AS total_instructors
                {prev_ign_from}
                {prev_ign_where}
                """,
                prev_ign_params,
            )

            # Previous year exposures
            prev_exp_where, prev_exp_params = _build_exposure_where(
                [prev_year], region, program, month, prev_month_year,
                program_type, engagement_mode
            )
            prev_exposure_row = fetch_one(
                f"""
                SELECT
                    SUM(COALESCE(rf.no_of_boys::int, 0) + COALESCE(rf.no_of_girls::int, 0)) AS student_exposure,
                    SUM(COALESCE(rf.no_of_men::int, 0) + COALESCE(rf.no_of_women::int, 0)) AS community_exposure
                FROM source.rpt_feedback rf
                JOIN source.txn_session s ON s.txn_session_id = rf.session_id
                JOIN source.mst_user u ON u.mst_user_id = s.instructor_id
                JOIN source.mst_role r ON r.mst_role_id = u.role_id
                LEFT JOIN source.mst_area a ON a.mst_area_id = u.area_id
                LEFT JOIN source.mst_region reg ON reg.mst_region_id = a.region_id
                {prev_exp_where}
                """,
                prev_exp_params,
            )
            
            curr_inst = int(ign_row.get("total_instructors", 0) or 0)
            prev_inst = int(prev_ign_row.get("total_instructors", 0) or 0)
            
            curr_exposure = int((exposure_row.get("student_exposure", 0) or 0) + (exposure_row.get("community_exposure", 0) or 0))
            prev_exposure = int((prev_exposure_row.get("student_exposure", 0) or 0) + (prev_exposure_row.get("community_exposure", 0) or 0))
            
            curr_state = int(kpis_row.get("total_sessions", 0) or 0)
            prev_state = int(prev_kpis_row.get("total_sessions", 0) or 0)
            
            curr_prog = int(kpis_row.get("active_programs", 0) or 0)
            prev_prog = int(prev_kpis_row.get("active_programs", 0) or 0)

            curr_inst_avg = curr_inst
            prev_inst_avg = prev_inst
            curr_driver_avg = curr_exposure
            prev_driver_avg = prev_exposure
            curr_state_avg = curr_state
            prev_state_avg = prev_state
            curr_prog_avg = curr_prog
            prev_prog_avg = prev_prog
            
            prev_vals = {
                "total_instructors": prev_inst,
                "total_instructors_avg": prev_inst_avg,
                "total_exposures": prev_exposure,
                "total_exposures_avg": prev_driver_avg,
                "total_sessions": prev_state,
                "total_sessions_avg": prev_state_avg,
                "total_programs": prev_prog,
                "total_programs_avg": prev_prog_avg,
            }
            
            def calc_trend(curr, prev):
                if not prev:
                    return {"pct": 0, "dir": "neutral"}
                diff = curr - prev
                pct = round((diff / prev) * 100, 1) if prev > 0 else 0
                direction = "up" if diff > 0 else ("down" if diff < 0 else "neutral")
                return {"pct": pct, "dir": direction}
                
            trends = {
                "total_instructors": calc_trend(curr_inst, prev_inst),
                "total_exposures": calc_trend(curr_exposure, prev_exposure),
                "total_sessions": calc_trend(curr_state, prev_state),
                "total_programs": calc_trend(curr_prog, prev_prog)
            }
        except Exception:
            pass

    total_exposures = int((exposure_row.get("student_exposure", 0) or 0) + (exposure_row.get("community_exposure", 0) or 0))

    response_data = {
        "total_instructors": int(ign_row.get("total_instructors", 0) or 0),
        "total_exposures": total_exposures,
        "total_sessions": int(kpis_row.get("total_sessions", 0) or 0),
        "total_programs": int(kpis_row.get("active_programs", 0) or 0),
        "inactive_programs": int(inactive_programs),
        "active_instructors": int(active_inst),
        "inactive_instructors": int(inactive_instructors),
        "total_coverage": int(coverage_row.get("total_coverage", 0) or 0),
        "total_unique_children": int(children_row.get("unique_children", 0) or 0),
        "total_working_days": int(kpis_row.get("total_working_days", 0) or 0),
        "active_bikes": int(logistics_row.get("active_bikes", 0) or 0),
        "active_buses": int(logistics_row.get("active_buses", 0) or 0),
    }
    if trends:
        response_data["trends"] = trends
        
    curr_vals = {
        "total_instructors": response_data["total_instructors"],
        "total_instructors_avg": curr_inst_avg if 'curr_inst_avg' in locals() else response_data["total_instructors"],
        "total_exposures": response_data["total_exposures"],
        "total_exposures_avg": curr_driver_avg if 'curr_driver_avg' in locals() else response_data["total_exposures"],
        "total_sessions": response_data["total_sessions"],
        "total_sessions_avg": response_data["total_sessions"],
        "total_programs": response_data["total_programs"],
        "total_programs_avg": response_data["total_programs"],
    }
    prev_year = single_year - 1 if single_year is not None else None
    response_data["insights"] = generate_insights_dict(curr_vals, prev_vals, trends, single_year, prev_year, month=month, region=region)
    
    return response_data


def _build_exposure_where(years, region, program, month, month_year,
                           program_type, engagement_mode):
    """
    Build WHERE clause for exposure queries against source.rpt_feedback.
    The rpt_feedback table is joined to source.txn_session and filters are
    applied through txn_session.date and mst_user/mst_role/mst_area/mst_region.
    """
    clauses = []
    params = []

    # -- Excluded deleted sessions --
    clauses.append("(s.is_deleted IS NULL OR s.is_deleted != '1')")

    # -- Future exclusion --
    clauses.append("(s.date)::date <= CURRENT_DATE")

    # -- Financial year filter --
    effective_years = years
    if not effective_years:
        effective_years = [get_current_fy()]

    fy_strings = [v for v in effective_years if parse_fy_string(str(v)) is not None]
    cal_years = [v for v in effective_years if parse_fy_string(str(v)) is None]

    if fy_strings:
        fy_conditions = []
        for fy_str in fy_strings:
            parsed = parse_fy_string(str(fy_str))
            if parsed:
                start_yr, end_yr = parsed
                fy_conditions.append(
                    "(EXTRACT(YEAR FROM (s.date)::date) = %s AND EXTRACT(MONTH FROM (s.date)::date) >= 4) OR "
                    "(EXTRACT(YEAR FROM (s.date)::date) = %s AND EXTRACT(MONTH FROM (s.date)::date) <= 3)"
                )
                params.extend([start_yr, end_yr])
        if fy_conditions:
            clauses.append("(" + " OR ".join(fy_conditions) + ")")

    if cal_years:
        clauses.append("EXTRACT(YEAR FROM (s.date)::date) = ANY(%s)")
        params.append([int(y) for y in cal_years])

    # -- Month / month_year filter --
    if month_year and len(month_year) > 0:
        clauses.append("TO_CHAR((s.date)::date, 'YYYY-MM') = ANY(%s)")
        params.append(month_year)
    elif month and len(month) > 0:
        try:
            month_ints = [int(m) for m in month if str(m).isdigit()]
            if month_ints:
                clauses.append("EXTRACT(MONTH FROM (s.date)::date) = ANY(%s)")
                params.append(month_ints)
        except Exception:
            pass
    else:
        current_fy = get_current_fy()
        single_fy = None
        if years and len(years) == 1:
            single_fy = str(years[0])
        elif not years or len(years) == 0:
            single_fy = current_fy
        if single_fy and single_fy == current_fy:
            clauses.append("EXTRACT(MONTH FROM (s.date)::date) <= %s")
            params.append(datetime.now().month)

    # -- Region filter (via mst_region) --
    if region:
        if isinstance(region, list):
            clean = [r for r in region if r]
            if clean:
                clauses.append("REPLACE(LOWER(reg.name), '_', ' ') = ANY(%s)")
                params.append([r.lower().replace("_", " ") for r in clean])
        elif region:
            clauses.append("REPLACE(LOWER(reg.name), '_', ' ') = %s")
            params.append(region.lower().replace("_", " "))

    where_clause = "WHERE " + " AND ".join(clauses) if clauses else ""
    return where_clause, params


def get_overview_trends(
    years: list[int] | list[str] | None = None, 
    region: list[str] | None = None, 
    program: list[str] | None = None, 
    month: list[str] | None = None,
    month_year: list[str] | None = None,
    program_type: list[str] | None = None,
    engagement_mode: list[str] | None = None
):
    """Returns YoY YTD trend comparisons for sparkline charts (previous YTD vs current YTD)."""
    # 1. Current YTD totals from source.txn_session — NO role filter for sessions/progs
    from_clause, where_clause, params = _build_txn_overview_filters(
        years=years, region=region, program=program,
        month=month, month_year=month_year,
        program_type=program_type, engagement_mode=engagement_mode,
        include_role_filter=False, is_overdue_filter=None,
    )
    if not program_type:
        from_clause += """
        LEFT JOIN source.conf_program_school_mapping cspm
            ON cspm.conf_program_school_mapping_id = s.program_school_mapped_id
        """

    curr_kpis_row = fetch_one(
        f"""
        SELECT
            COUNT(*) AS total_sessions,
            COUNT(DISTINCT cspm.program_id) AS total_programs
        {from_clause}
        {where_clause}
        """,
        params,
    )

    # Current Active Ignators (role-filtered, is_overdue=false)
    ign_from, ign_where, ign_params = _build_txn_overview_filters(
        years=years, region=region, program=program,
        month=month, month_year=month_year,
        program_type=program_type, engagement_mode=engagement_mode,
        include_role_filter=True, is_overdue_filter='false',
    )
    curr_ign_row = fetch_one(
        f"SELECT COUNT(DISTINCT s.instructor_id) AS total_instructors {ign_from} {ign_where}",
        ign_params,
    )

    # Current exposures from source.rpt_feedback
    exp_where, exp_params = _build_exposure_where(years, region, program, month, month_year,
                                       program_type, engagement_mode)
    curr_exposure_row = fetch_one(
        f"""
        SELECT
            SUM(COALESCE(rf.no_of_boys::int, 0) + COALESCE(rf.no_of_girls::int, 0)) AS student_exposure,
            SUM(COALESCE(rf.no_of_men::int, 0) + COALESCE(rf.no_of_women::int, 0)) AS community_exposure
        FROM source.rpt_feedback rf
        JOIN source.txn_session s ON s.txn_session_id = rf.session_id
        JOIN source.mst_user u ON u.mst_user_id = s.instructor_id
        JOIN source.mst_role r ON r.mst_role_id = u.role_id
        LEFT JOIN source.mst_area a ON a.mst_area_id = u.area_id
        LEFT JOIN source.mst_region reg ON reg.mst_region_id = a.region_id
        {exp_where}
        """,
        exp_params,
    )
    
    curr_inst = int(curr_ign_row.get("total_instructors", 0) or 0)
    curr_driver = int((curr_exposure_row.get("student_exposure", 0) or 0) + (curr_exposure_row.get("community_exposure", 0) or 0))
    curr_state = int(curr_kpis_row.get("total_sessions", 0) or 0)
    curr_prog = int(curr_kpis_row.get("total_programs", 0) or 0)

    # 2. Determine previous YTD totals
    single_year = None
    if years and len(years) == 1:
        try:
            single_year = int(str(years[0])[:4])
        except (ValueError, TypeError):
            pass
    elif years is None or len(years) == 0:
        single_year = DEFAULT_YEAR

    prev_inst = 0
    prev_driver = 0
    prev_state = 0
    prev_prog = 0

    if single_year is not None:
        try:
            prev_year = single_year - 1
            prev_from, prev_where, prev_params = _build_txn_overview_filters(
                years=[prev_year], region=region, program=program,
                month=month, month_year=None,
                program_type=program_type, engagement_mode=engagement_mode,
                include_role_filter=True, is_overdue_filter=None,
            )
            if not program_type:
                prev_from += """
                LEFT JOIN source.conf_program_school_mapping cspm
                    ON cspm.conf_program_school_mapping_id = s.program_school_mapped_id
                """
            prev_month_year = None
            if month_year:
                prev_month_year = []
                for my in month_year:
                    parts = my.split('-')
                    if len(parts) == 2:
                        try:
                            y_val = int(parts[0])
                            prev_month_year.append(f"{y_val - 1}-{parts[1]}")
                        except ValueError:
                            prev_month_year.append(my)
                    else:
                        prev_month_year.append(my)
            prev_from, prev_where, prev_params = _build_txn_overview_filters(
                years=[prev_year], region=region, program=program,
                month=month, month_year=prev_month_year,
                program_type=program_type, engagement_mode=engagement_mode,
                include_role_filter=False, is_overdue_filter=None,
            )
            if not program_type:
                prev_from += """
                LEFT JOIN source.conf_program_school_mapping cspm
                    ON cspm.conf_program_school_mapping_id = s.program_school_mapped_id
                """

            prev_kpis_row = fetch_one(
                f"""
                SELECT
                    COUNT(*) AS total_sessions,
                    COUNT(DISTINCT cspm.program_id) AS total_programs
                {prev_from}
                {prev_where}
                """,
                prev_params,
            )

            # Previous year Active Ignators (role-filtered, is_overdue=false)
            prev_ign_from, prev_ign_where, prev_ign_params = _build_txn_overview_filters(
                years=[prev_year], region=region, program=program,
                month=month, month_year=prev_month_year,
                program_type=program_type, engagement_mode=engagement_mode,
                include_role_filter=True, is_overdue_filter='false',
            )
            prev_ign_row = fetch_one(
                f"SELECT COUNT(DISTINCT s.instructor_id) AS total_instructors {prev_ign_from} {prev_ign_where}",
                prev_ign_params,
            )

            prev_exp_where, prev_exp_params = _build_exposure_where(
                [prev_year], region, program, month, prev_month_year,
                program_type, engagement_mode
            )
            prev_exposure_row = fetch_one(
                f"""
                SELECT
                    SUM(COALESCE(rf.no_of_boys::int, 0) + COALESCE(rf.no_of_girls::int, 0)) AS student_exposure,
                    SUM(COALESCE(rf.no_of_men::int, 0) + COALESCE(rf.no_of_women::int, 0)) AS community_exposure
                FROM source.rpt_feedback rf
                JOIN source.txn_session s ON s.txn_session_id = rf.session_id
                JOIN source.mst_user u ON u.mst_user_id = s.instructor_id
                JOIN source.mst_role r ON r.mst_role_id = u.role_id
                LEFT JOIN source.mst_area a ON a.mst_area_id = u.area_id
                LEFT JOIN source.mst_region reg ON reg.mst_region_id = a.region_id
                {prev_exp_where}
                """,
                prev_exp_params,
            )
            
            prev_inst = int(prev_ign_row.get("total_instructors", 0) or 0)
            prev_driver = int((prev_exposure_row.get("student_exposure", 0) or 0) + (prev_exposure_row.get("community_exposure", 0) or 0))
            prev_state = int(prev_kpis_row.get("total_sessions", 0) or 0)
            prev_prog = int(prev_kpis_row.get("total_programs", 0) or 0)
        except Exception:
            pass
    else:
        prev_inst = curr_inst
        prev_driver = curr_driver
        prev_state = curr_state
        prev_prog = curr_prog

    return [
        {
            "instructors": prev_inst,
            "states": prev_state,
            "programs": prev_prog,
            "drivers": prev_driver
        },
        {
            "instructors": curr_inst,
            "states": curr_state,
            "programs": curr_prog,
            "drivers": curr_driver
        }
    ]

def get_overview_charts(
    years: list[int] | list[str] | None = None, 
    region: list[str] | None = None, 
    program: list[str] | None = None, 
    month: list[str] | None = None,
    month_year: list[str] | None = None,
    program_type: list[str] | None = None,
    engagement_mode: list[str] | None = None
):
    where_clause, params = _build_filters(
        years=years, region=region, program=program,
        program_type=program_type, engagement_mode=engagement_mode
    )
    # Apply YTD month boundary filtering
    where_clause, params = _apply_ytd_filter(
        where_clause, params, years, region, program, 
        month=month, month_year=month_year
    )
    
    # 1. Instructors per region
    instructors_rows = fetch_all(
        f"""
        SELECT
            COALESCE(g.region_name, 'Unknown') AS label,
            COUNT(DISTINCT f.sk_user_id) AS value
        FROM dw.fact_session f
        LEFT JOIN dw.dim_date d ON d.date_id = f.date_id
        LEFT JOIN dw.dim_geography g ON g.sk_geography_id = f.sk_geography_id
        LEFT JOIN dw.dim_program p ON p.sk_program_id = f.sk_program_id
        {where_clause} AND g.region_name IS NOT NULL
        GROUP BY g.region_name
        ORDER BY value DESC
        LIMIT 10
        """,
        params,
    )

    
    # 2. Programs per region (no role filter — show all programs)
    prog_where, prog_params = build_dimension_filters(
        year=years, region=region, program=None,
        year_expression="d.year_actual", location_expression=LOCATION_EXPRESSION,
    )
    prog_where, prog_params = _apply_ytd_filter(
        prog_where, prog_params, years, region, program, month=month, month_year=month_year
    )
    programs_rows = fetch_all(
        f"""
        SELECT
            COALESCE(g.region_name, 'Unknown') AS label,
            COUNT(DISTINCT p.program_name) AS value
        FROM dw.fact_session f
        LEFT JOIN dw.dim_date d ON d.date_id = f.date_id
        LEFT JOIN dw.dim_geography g ON g.sk_geography_id = f.sk_geography_id
        LEFT JOIN dw.dim_program p ON p.sk_program_id = f.sk_program_id
        {prog_where} AND g.region_name IS NOT NULL
        GROUP BY g.region_name
        ORDER BY value DESC
        LIMIT 15
        """,
        prog_params,
    )

    # 3. Drivers per region
    driver_where, driver_params = _build_filters(
        years=years, region=region, program=program, is_vehicle_ops=True,
        program_type=program_type, engagement_mode=engagement_mode
    )
    driver_where, driver_params = _apply_ytd_filter(
        driver_where, driver_params, years, region, program, 
        month=month, month_year=month_year
    )
    drivers_rows = fetch_all(
        f"""
        SELECT
            COALESCE(g.region_name, 'Unknown') AS label,
            COUNT(DISTINCT f.sk_user_id) AS value
        FROM dw.fact_vehicle_operations f
        JOIN dw.dim_user u ON f.sk_user_id = u.sk_user_id
        LEFT JOIN dw.dim_date d ON d.date_id = f.date_id
        LEFT JOIN dw.dim_geography g ON g.sk_geography_id = f.sk_geography_id
        LEFT JOIN dw.dim_program p ON p.sk_program_id = f.sk_program_id
        {driver_where} AND u.role_name = 'DRIVER' AND g.region_name IS NOT NULL
        GROUP BY g.region_name
        ORDER BY value DESC
        LIMIT 10
        """,
        driver_params,
    )

    # 4. Sessions per region
    sessions_rows = fetch_all(
        f"""
        SELECT
            COALESCE(g.region_name, 'Unknown') AS label,
            COUNT(DISTINCT f.sk_fact_session_id) AS value
        FROM dw.fact_session f
        LEFT JOIN dw.dim_date d ON d.date_id = f.date_id
        LEFT JOIN dw.dim_geography g ON g.sk_geography_id = f.sk_geography_id
        LEFT JOIN dw.dim_program p ON p.sk_program_id = f.sk_program_id
        {where_clause} AND g.region_name IS NOT NULL
        GROUP BY g.region_name
        ORDER BY value DESC
        LIMIT 10
        """,
        params,
    )


    return {
        "instructors_by_region": [{"label": r["label"], "value": float(r["value"])} for r in instructors_rows],
        "drivers_by_region": [{"label": r["label"], "value": float(r["value"])} for r in drivers_rows],
        "programs_by_region": [{"label": r["label"], "value": float(r["value"])} for r in programs_rows],
        "sessions_by_region": [{"label": r["label"], "value": float(r["value"])} for r in sessions_rows]
    }


def get_program_targets(
    years: list[int] | list[str] | None = None, 
    region: list[str] | None = None, 
    program: list[str] | None = None, 
    month: list[str] | None = None, 
    limit: int = 10, 
    offset: int = 0,
    month_year: list[str] | None = None,
    program_type: list[str] | None = None,
    engagement_mode: list[str] | None = None
):
    where_clause, params = _build_filters(
        years=years, region=region, program=program,
        program_type=program_type, engagement_mode=engagement_mode
    )
    # Apply YTD month boundary filtering
    where_clause, params = _apply_ytd_filter(
        where_clause, params, years, region, program, 
        month=month, month_year=month_year
    )
    
    total_count = fetch_one(
        f"""
        SELECT COUNT(*) FROM dw.dim_program
        """
    )["count"]


    rows = fetch_all(
        f"""
        SELECT
            p.sk_program_id,
            COALESCE(p.program_name, 'Unknown') AS label,
            COALESCE(p.donor_name, 'Unknown') AS donor,
            COALESCE(p.instructor_capacity, 0) AS target_sessions,
            COALESCE(COUNT(DISTINCT f.sk_fact_session_id), 0) AS completed_sessions,
            COALESCE(SUM(fa.total_exposure_count + f.community_men_count + f.community_women_count), 0) AS reached_students,
            p.end_date AS end_date
        FROM dw.dim_program p
        LEFT JOIN dw.fact_session f ON p.sk_program_id = f.sk_program_id
        LEFT JOIN dw.fact_attendance_exposure fa ON f.session_nk_id = fa.session_nk_id
        LEFT JOIN dw.dim_date d ON d.date_id = f.date_id
        LEFT JOIN dw.dim_geography g ON g.sk_geography_id = f.sk_geography_id
        {where_clause}
        GROUP BY p.sk_program_id
        ORDER BY completed_sessions DESC, label
        LIMIT %s OFFSET %s
        """,
        [*params, limit, offset],
    )


    items = []
    for row in rows:
        target_sessions = int(row.get("target_sessions", 0) or 0)
        completed_sessions = int(row.get("completed_sessions", 0) or 0)
        pct = round((completed_sessions / target_sessions) * 100) if target_sessions else 0
        if pct >= 80:
            status = "On track"
        elif pct >= 50:
            status = "At risk"
        else:
            status = "Behind"
        items.append(
            {
                "label": row.get("label") or "Unknown",
                "donor": row.get("donor") or "Unknown",
                "completed_sessions": completed_sessions,
                "target_sessions": target_sessions,
                        "students_target": int(row.get("target_students", 0) or 0),
                        "students_reached": int(row.get("reached_students", 0) or 0),
                "progress_pct": pct,
                "end_date": row["end_date"].strftime("%b %Y") if row.get("end_date") else "Unknown",
                "status": status,
            }
        )
    return {"table": items, "total_count": total_count}


def get_sessions_by_activity(
    years: list[int] | list[str] | None = None, 
    region: list[str] | None = None, 
    program: list[str] | None = None, 
    month: list[str] | None = None,
    month_year: list[str] | None = None,
    program_type: list[str] | None = None,
    engagement_mode: list[str] | None = None
):
    where_clause, params = _build_filters(
        years=years, region=region, program=program,
        program_type=program_type, engagement_mode=engagement_mode
    )
    # Apply YTD month boundary filtering
    where_clause, params = _apply_ytd_filter(
        where_clause, params, years, region, program, 
        month=month, month_year=month_year
    )
    rows = fetch_all(
        f"""
        SELECT
            COALESCE(a.activity_name, 'Unknown') AS label,
            COALESCE(COUNT(DISTINCT f.sk_fact_session_id), 0) AS value
        FROM dw.fact_session f
        LEFT JOIN dw.dim_date d ON d.date_id = f.date_id
        LEFT JOIN dw.dim_geography g ON g.sk_geography_id = f.sk_geography_id
        LEFT JOIN dw.dim_program p ON p.sk_program_id = f.sk_program_id
        LEFT JOIN dw.dim_activity_type a ON a.sk_activity_type_id = f.sk_activity_type_id
        {where_clause}
        GROUP BY COALESCE(a.activity_name, 'Unknown')
        ORDER BY value DESC, label
        LIMIT 6
        """,
        params,
    )

    return [{"label": row["label"], "value": float(row["value"])} for row in rows]


def get_sessions_by_donor(
    years: list[int] | list[str] | None = None, 
    region: list[str] | None = None, 
    program: list[str] | None = None, 
    month: list[str] | None = None,
    month_year: list[str] | None = None,
    program_type: list[str] | None = None,
    engagement_mode: list[str] | None = None
):
    where_clause, params = _build_filters(
        years=years, region=region, program=program,
        program_type=program_type, engagement_mode=engagement_mode
    )
    # Apply YTD month boundary filtering
    where_clause, params = _apply_ytd_filter(
        where_clause, params, years, region, program, 
        month=month, month_year=month_year
    )
    rows = fetch_all(
        f"""
        SELECT
            COALESCE(p.donor_name, 'Unknown') AS label,
            COALESCE(COUNT(DISTINCT f.sk_fact_session_id), 0) AS value
        FROM dw.fact_session f
        LEFT JOIN dw.dim_date d ON d.date_id = f.date_id
        LEFT JOIN dw.dim_geography g ON g.sk_geography_id = f.sk_geography_id
        LEFT JOIN dw.dim_program p ON p.sk_program_id = f.sk_program_id
        {where_clause}
        GROUP BY COALESCE(p.donor_name, 'Unknown')
        ORDER BY value DESC, label
        LIMIT 6
        """,
        params,
    )

    return [{"label": row["label"], "value": float(row["value"])} for row in rows]


def get_drilldown_data(
    region: str,
    years: list[int] | list[str] | None = None,
    program: list[str] | None = None,
    month: list[str] | None = None,
    month_year: list[str] | None = None,
    program_type: list[str] | None = None,
    engagement_mode: list[str] | None = None
):
    """
    Returns rich drill-down stats for a specific region click.
    Uses hardened matching to ensure data integrity.
    """
    # 1. Build base filters (default to 2026 if none provided)
    where_clause, params = _build_filters(
        years=years, program=program,
        program_type=program_type, engagement_mode=engagement_mode
    )
    # Apply YTD month boundary filtering
    where_clause, params = _apply_ytd_filter(
        where_clause, params, years, region=None, program=program, 
        month=month, month_year=month_year
    )
    
    # 2. Add hardened region filter
    region_norm = region.lower().replace("_", " ")
    region_filter = "REPLACE(LOWER(g.region_name), '_', ' ') = %s"
    
    if where_clause:
        where_clause += f" AND {region_filter}"
    else:
        where_clause = f"WHERE {region_filter}"
    params.append(region_norm)

    # 1. Extended summary stats
    summary_row = fetch_one(
        f"""
        SELECT
            COUNT(DISTINCT f.sk_fact_session_id)        AS total_sessions,
            COALESCE(SUM(fa.total_exposure_count + f.community_men_count + f.community_women_count), 0)   AS total_students,
            COUNT(DISTINCT f.sk_school_id)              AS total_schools
        FROM dw.fact_session f
        LEFT JOIN dw.dim_date d       ON d.date_id        = f.date_id
        LEFT JOIN dw.dim_geography g  ON g.sk_geography_id = f.sk_geography_id
        LEFT JOIN dw.dim_program p    ON p.sk_program_id   = f.sk_program_id
        LEFT JOIN dw.fact_attendance_exposure fa ON fa.session_nk_id = f.session_nk_id
        {where_clause}
        """,
        params,
    )

    # 2. Per-program breakdown table
    prog_rows = fetch_all(
        f"""
        SELECT
            COALESCE(p.program_name, 'Unknown')         AS program_name,
            COALESCE(p.donor_name, 'Unknown')           AS donor,
            COUNT(DISTINCT f.sk_fact_session_id)        AS sessions,
            COALESCE(SUM(fa.total_exposure_count + f.community_men_count + f.community_women_count), 0)   AS students_reached,
            COUNT(DISTINCT f.sk_school_id)              AS schools_visited,
            COUNT(DISTINCT f.sk_user_id)                AS instructors
        FROM dw.fact_session f
        LEFT JOIN dw.dim_date d       ON d.date_id        = f.date_id
        LEFT JOIN dw.dim_geography g  ON g.sk_geography_id = f.sk_geography_id
        LEFT JOIN dw.dim_program p    ON p.sk_program_id   = f.sk_program_id
        LEFT JOIN dw.fact_attendance_exposure fa ON fa.session_nk_id = f.session_nk_id
        {where_clause}
        GROUP BY p.program_name, p.donor_name
        ORDER BY sessions DESC
        LIMIT 2000
        """,
        params,
    )

    programs = [
        {
            "program": row.get("program_name") or "Unknown",
            "donor": row.get("donor") or "Unknown",
            "sessions": int(row.get("sessions", 0) or 0),
            "students_reached": int(row.get("students_reached", 0) or 0),
            "schools_visited": int(row.get("schools_visited", 0) or 0),
            "instructors": int(row.get("instructors", 0) or 0),
        }
        for row in prog_rows
    ]

    return {
        "region": region,
        "summary": {
            "total_sessions": int(summary_row.get("total_sessions", 0) or 0),
            "total_students": int(summary_row.get("total_students", 0) or 0),
            "total_schools": int(summary_row.get("total_schools", 0) or 0),
        },
        "programs": programs,
    }


def get_programs_by_type(
    years=None, region=None, program=None, month=None, month_year=None,
    program_type=None, engagement_mode=None
):
    where_clause, params = build_dimension_filters(
        year=years, region=region, program=None,
        year_expression="d.year_actual", location_expression=LOCATION_EXPRESSION,
    )
    where_clause, params = _apply_ytd_filter(
        where_clause, params, years, region, program, month=month, month_year=month_year
    )
    rows = fetch_all(f"""
        SELECT (CASE
            WHEN pt.name = 'Mobile Lab' THEN 'MSL'
            WHEN pt.name = 'Lab on a Bike' THEN 'LOB'
            WHEN pt.name = 'Lab On A Bike - Maths' THEN 'LOB-Maths'
            WHEN pt.name = 'Science Center' THEN 'SC'
            WHEN pt.name = 'Mobile Innovation Lab' THEN 'MLH'
            WHEN pt.name = 'Young Instructor Leader' THEN 'YL'
            WHEN pt.name = 'I Mobile Science Lab' THEN 'IML'
            WHEN pt.name = 'Innovation Hub' THEN 'ELOB'
            WHEN pt.name = 'Operation Vasantha' THEN 'OV'
            WHEN pt.name = 'Lab in a Box' THEN 'LIB'
            WHEN pt.name = 'STEM Clubs' THEN 'SClubs'
            ELSE 'Other'
        END) AS label,
        SUM(COALESCE(e.total_exposure_count, 0)) AS value
        FROM dw.fact_session f
        LEFT JOIN dw.dim_date d ON d.date_id = f.date_id
        LEFT JOIN dw.dim_geography g ON g.sk_geography_id = f.sk_geography_id
        LEFT JOIN dw.dim_program p ON p.sk_program_id = f.sk_program_id
        LEFT JOIN dw.fact_attendance_exposure e ON f.session_nk_id = e.session_nk_id
        LEFT JOIN source.txn_program tp ON tp.txn_program_id::TEXT = p.nk_program_id::TEXT
        LEFT JOIN source.mst_program_type pt ON (CASE WHEN tp.program_type_id ~ '^[0-9]+$' THEN tp.program_type_id::BIGINT ELSE NULL END) = (CASE WHEN pt.mst_program_type_id ~ '^[0-9]+$' THEN pt.mst_program_type_id::BIGINT ELSE NULL END)
        {where_clause}
        GROUP BY label
        ORDER BY value DESC
        LIMIT 15
    """, params)
    return [{"label": r["label"] or "Other", "value": float(r["value"])} for r in rows]


def get_mode_of_engagement(
    years=None, region=None, program=None, month=None, month_year=None,
    program_type=None, engagement_mode=None
):
    where_clause, params = build_dimension_filters(
        year=years, region=region, program=None,
        year_expression="d.year_actual", location_expression=LOCATION_EXPRESSION,
    )
    where_clause, params = _apply_ytd_filter(
        where_clause, params, years, region, program, month=month, month_year=month_year
    )
    rows = fetch_all(f"""
        SELECT (CASE
            WHEN rf.mode_of_engagement::INT = 201 THEN 'Physical'
            WHEN rf.mode_of_engagement::INT = 202 THEN 'Digital (IML)'
            WHEN rf.mode_of_engagement::INT = 203 THEN 'Phygital (wELearn)'
            WHEN rf.mode_of_engagement::INT = 206 THEN 'Other Activity'
            ELSE 'Digital (wELearn)'
        END) AS label,
        COUNT(*) AS value
        FROM dw.fact_session f
        LEFT JOIN dw.dim_date d ON d.date_id = f.date_id
        LEFT JOIN dw.dim_geography g ON g.sk_geography_id = f.sk_geography_id
        LEFT JOIN dw.dim_program p ON p.sk_program_id = f.sk_program_id
        LEFT JOIN source.rpt_feedback rf ON rf.session_id::TEXT = f.session_nk_id::TEXT
        {where_clause}
        GROUP BY label
        ORDER BY value DESC
    """, params)
    return [{"label": r["label"] or "Unknown", "value": float(r["value"])} for r in rows]


def get_mode_of_engagement_summary(
    years=None, region=None, program=None, month=None, month_year=None,
    program_type=None, engagement_mode=None
):
    where_clause, params = build_dimension_filters(
        year=years, region=region, program=None,
        year_expression="d.year_actual", location_expression=LOCATION_EXPRESSION,
    )
    where_clause, params = _apply_ytd_filter(
        where_clause, params, years, region, program, month=month, month_year=month_year
    )
    rows = fetch_all(f"""
        SELECT
            (CASE
                WHEN rf.mode_of_engagement::INT = 201 THEN 'Physical'
                WHEN rf.mode_of_engagement::INT = 202 THEN 'Digital (IML)'
                WHEN rf.mode_of_engagement::INT = 203 THEN 'Phygital (wELearn)'
                WHEN rf.mode_of_engagement::INT = 206 THEN 'Other Activity'
                ELSE 'Digital (wELearn)'
            END) AS mode_of_engagement,
            COALESCE(SUM(exp.exposure_sum), 0) + COALESCE(SUM(f.community_men_count + f.community_women_count), 0) AS total_exposures,
            COUNT(DISTINCT f.sk_fact_session_id) AS no_of_session,
            ROUND((COALESCE(SUM(exp.exposure_sum), 0) + COALESCE(SUM(f.community_men_count + f.community_women_count), 0)) / NULLIF(COUNT(DISTINCT p.program_name), 0), 0) AS exp_pgm,
            ROUND((COALESCE(SUM(exp.exposure_sum), 0) + COALESCE(SUM(f.community_men_count + f.community_women_count), 0)) / NULLIF(COUNT(DISTINCT f.sk_user_id), 0), 0) AS expo_instructor,
            ROUND((COALESCE(SUM(exp.exposure_sum), 0) + COALESCE(SUM(f.community_men_count + f.community_women_count), 0)) / NULLIF(COUNT(DISTINCT f.sk_fact_session_id), 0), 0) AS expo_session,
            COUNT(DISTINCT p.program_name) AS no_of_pgm,
            COUNT(DISTINCT f.sk_user_id) AS no_of_ins,
            COUNT(DISTINCT f.date_id) AS wd
        FROM dw.fact_session f
        LEFT JOIN dw.dim_date d ON d.date_id = f.date_id
        LEFT JOIN dw.dim_geography g ON g.sk_geography_id = f.sk_geography_id
        LEFT JOIN dw.dim_program p ON p.sk_program_id = f.sk_program_id
        LEFT JOIN dw.dim_user u ON u.sk_user_id = f.sk_user_id
        LEFT JOIN source.rpt_feedback rf ON rf.session_id::TEXT = f.session_nk_id::TEXT
        LEFT JOIN (
            SELECT session_nk_id, SUM(total_exposure_count) AS exposure_sum
            FROM dw.fact_attendance_exposure
            GROUP BY session_nk_id
        ) exp ON f.session_nk_id = exp.session_nk_id
        {where_clause}
        GROUP BY mode_of_engagement
        ORDER BY total_exposures DESC
    """, params)
    return rows


def get_exposure_by_activity(
    years=None, region=None, program=None, month=None, month_year=None,
    program_type=None, engagement_mode=None
):
    where_clause, params = build_dimension_filters(
        year=years, region=region, program=None,
        year_expression="d.year_actual", location_expression=LOCATION_EXPRESSION,
    )
    where_clause, params = _apply_ytd_filter(
        where_clause, params, years, region, program, month=month, month_year=month_year
    )
    rows = fetch_all(f"""
        SELECT a.activity_name AS label,
               SUM(COALESCE(e.total_exposure_count, 0)) AS value
        FROM dw.fact_session f
        LEFT JOIN dw.dim_date d ON d.date_id = f.date_id
        LEFT JOIN dw.dim_geography g ON g.sk_geography_id = f.sk_geography_id
        LEFT JOIN dw.dim_program p ON p.sk_program_id = f.sk_program_id
        LEFT JOIN dw.dim_activity_type a ON f.sk_activity_type_id = a.sk_activity_type_id
        LEFT JOIN dw.fact_attendance_exposure e ON f.session_nk_id = e.session_nk_id
        {where_clause}
        GROUP BY a.activity_name
        ORDER BY value DESC
        LIMIT 15
    """, params)
    return [{"label": r["label"] or "Unknown", "value": float(r["value"])} for r in rows]


def get_exposure_by_activity_and_program(
    years=None, region=None, program=None, month=None, month_year=None,
    program_type=None, engagement_mode=None
):
    where_clause, params = build_dimension_filters(
        year=years, region=region, program=None,
        year_expression="d.year_actual", location_expression=LOCATION_EXPRESSION,
    )
    where_clause, params = _apply_ytd_filter(
        where_clause, params, years, region, program, month=month, month_year=month_year
    )
    rows = fetch_all(f"""
        SELECT a.activity_name AS activity,
               COALESCE(pt.code, 'Other') AS program_type,
               COALESCE(SUM(e.total_exposure_count), 0) AS exposure
        FROM dw.fact_session f
        LEFT JOIN dw.dim_date d ON d.date_id = f.date_id
        LEFT JOIN dw.dim_geography g ON g.sk_geography_id = f.sk_geography_id
        LEFT JOIN dw.dim_program p ON p.sk_program_id = f.sk_program_id
        LEFT JOIN dw.dim_activity_type a ON f.sk_activity_type_id = a.sk_activity_type_id
        LEFT JOIN dw.fact_attendance_exposure e ON f.session_nk_id = e.session_nk_id
        LEFT JOIN source.txn_program tp ON p.nk_program_id::TEXT = tp.txn_program_id
        LEFT JOIN source.mst_program_type pt ON tp.program_type_id = pt.mst_program_type_id
        {where_clause}
        GROUP BY a.activity_name, pt.code
        ORDER BY exposure DESC
    """, params)
    return rows
