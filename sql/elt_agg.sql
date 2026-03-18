WITH monthly_region_base AS (
    SELECT
        DATE_TRUNC('month', d.date)::date AS period_start,
        f.location_key,
        SUM(f.session_count) AS sessions_count,
        SUM(COALESCE(e.students_total, 0)) AS students_reached,
        SUM(COALESCE(e.teachers_count, 0)) AS teachers_trained,
        COUNT(DISTINCT f.location_key) AS schools_covered,
        SUM(COALESCE(e.community_men, 0) + COALESCE(e.community_women, 0)) AS community_reach
    FROM fact_session_event f
    JOIN dim_date d ON d.date_key = f.date_key
    LEFT JOIN fact_exposure e ON e.session_key = f.session_key
    GROUP BY DATE_TRUNC('month', d.date), f.location_key
)
INSERT INTO fact_monthly_region_impact (
    month_key,
    location_key,
    sessions,
    students_reached,
    teachers_trained,
    schools_covered,
    community_reach
)
SELECT
    dd.date_key,
    m.location_key,
    m.sessions_count,
    m.students_reached,
    m.teachers_trained,
    m.schools_covered,
    m.community_reach
FROM monthly_region_base m
JOIN dim_date dd ON dd.date = m.period_start;

WITH weekly_program_base AS (
    SELECT
        DATE_TRUNC('week', d.date)::date AS period_start,
        f.program_key,
        f.location_key,
        SUM(f.session_count) AS sessions_count,
        SUM(COALESCE(e.students_total, 0)) AS students_reached,
        SUM(COALESCE(e.teachers_count, 0)) AS teachers_trained,
        COUNT(DISTINCT f.location_key) AS schools_covered,
        SUM(COALESCE(e.community_men, 0) + COALESCE(e.community_women, 0)) AS community_reach,
        AVG(COALESCE(f.session_duration, 0)) AS avg_session_duration
    FROM fact_session_event f
    JOIN dim_date d ON d.date_key = f.date_key
    LEFT JOIN fact_exposure e ON e.session_key = f.session_key
    GROUP BY DATE_TRUNC('week', d.date), f.program_key, f.location_key
)
INSERT INTO fact_weekly_program_metrics (
    week_key,
    program_key,
    location_key,
    sessions,
    students_reached,
    teachers_trained,
    schools_covered,
    community_reach,
    avg_session_duration
)
SELECT
    dd.date_key,
    w.program_key,
    w.location_key,
    w.sessions_count,
    w.students_reached,
    w.teachers_trained,
    w.schools_covered,
    w.community_reach,
    w.avg_session_duration
FROM weekly_program_base w
JOIN dim_date dd ON dd.date = w.period_start;

WITH monthly_instructor_base AS (
    SELECT
        DATE_TRUNC('month', d.date)::date AS period_start,
        f.instructor_key,
        f.location_key,
        SUM(f.session_count) AS sessions_conducted,
        SUM(COALESCE(e.students_total, 0)) AS students_reached,
        COUNT(DISTINCT f.location_key) AS schools_visited,
        AVG(COALESCE(f.session_duration, 0)) AS avg_session_duration,
        SUM(CASE WHEN f.is_overdue THEN 1 ELSE 0 END) AS overdue_sessions
    FROM fact_session_event f
    JOIN dim_date d ON d.date_key = f.date_key
    LEFT JOIN fact_exposure e ON e.session_key = f.session_key
    GROUP BY DATE_TRUNC('month', d.date), f.instructor_key, f.location_key
)
INSERT INTO fact_instructor_productivity (
    month_key,
    instructor_key,
    location_key,
    sessions_conducted,
    students_reached,
    schools_visited,
    avg_session_duration,
    overdue_sessions
)
SELECT
    dd.date_key,
    i.instructor_key,
    i.location_key,
    i.sessions_conducted,
    i.students_reached,
    i.schools_visited,
    i.avg_session_duration,
    i.overdue_sessions
FROM monthly_instructor_base i
JOIN dim_date dd ON dd.date = i.period_start;
