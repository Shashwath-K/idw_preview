WITH monthly_region_base AS (
    SELECT
        DATE_TRUNC('month', d.date_value)::date AS period_start,
        f.location_key,
        SUM(f.session_count) AS sessions_count,
        SUM(COALESCE(e.students_total, 0)) AS students_reached,
        SUM(COALESCE(e.boys_count, 0)) AS boys_count,
        SUM(COALESCE(e.girls_count, 0)) AS girls_count,
        SUM(COALESCE(e.teachers_count, 0)) AS teachers_count
    FROM fact_session_event f
    JOIN dim_date d ON d.date_key = f.date_key
    LEFT JOIN fact_exposure e ON e.session_key = f.session_key
    GROUP BY DATE_TRUNC('month', d.date_value), f.location_key
)
INSERT INTO fact_monthly_region_impact (
    date_key,
    location_key,
    sessions_count,
    students_reached,
    boys_count,
    girls_count,
    teachers_count
)
SELECT
    dd.date_key,
    m.location_key,
    m.sessions_count,
    m.students_reached,
    m.boys_count,
    m.girls_count,
    m.teachers_count
FROM monthly_region_base m
JOIN dim_date dd ON dd.date_value = m.period_start;

WITH weekly_program_base AS (
    SELECT
        DATE_TRUNC('week', d.date_value)::date AS period_start,
        f.program_key,
        SUM(f.session_count) AS sessions_count,
        SUM(COALESCE(e.students_total, 0)) AS students_reached,
        MAX(COALESCE(p.target_sessions, 0)) AS target_sessions,
        MAX(COALESCE(p.target_students, 0)) AS target_students
    FROM fact_session_event f
    JOIN dim_date d ON d.date_key = f.date_key
    LEFT JOIN fact_exposure e ON e.session_key = f.session_key
    LEFT JOIN dim_program p ON p.program_key = f.program_key
    GROUP BY DATE_TRUNC('week', d.date_value), f.program_key
)
INSERT INTO fact_weekly_program_metrics (
    date_key,
    program_key,
    sessions_count,
    students_reached,
    target_sessions,
    target_students
)
SELECT
    dd.date_key,
    w.program_key,
    w.sessions_count,
    w.students_reached,
    w.target_sessions,
    w.target_students
FROM weekly_program_base w
JOIN dim_date dd ON dd.date_value = w.period_start;

WITH monthly_instructor_base AS (
    SELECT
        DATE_TRUNC('month', d.date_value)::date AS period_start,
        f.instructor_key,
        SUM(f.session_count) AS sessions_conducted,
        SUM(COALESCE(e.students_total, 0)) AS students_reached,
        AVG(COALESCE(f.session_duration, 0)) AS avg_session_duration,
        SUM(CASE WHEN f.is_overdue THEN 1 ELSE 0 END) AS overdue_sessions
    FROM fact_session_event f
    JOIN dim_date d ON d.date_key = f.date_key
    LEFT JOIN fact_exposure e ON e.session_key = f.session_key
    GROUP BY DATE_TRUNC('month', d.date_value), f.instructor_key
)
INSERT INTO fact_instructor_productivity (
    date_key,
    instructor_key,
    sessions_conducted,
    students_reached,
    avg_session_duration,
    overdue_sessions
)
SELECT
    dd.date_key,
    i.instructor_key,
    i.sessions_conducted,
    i.students_reached,
    i.avg_session_duration,
    i.overdue_sessions
FROM monthly_instructor_base i
JOIN dim_date dd ON dd.date_value = i.period_start;
