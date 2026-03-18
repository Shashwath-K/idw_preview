TRUNCATE TABLE
    fact_session_attribute,
    fact_exposure,
    fact_session_event,
    fact_monthly_region_impact,
    fact_weekly_program_metrics,
    fact_instructor_productivity,
    dim_program,
    dim_donor,
    dim_location,
    dim_instructor,
    dim_activity,
    dim_shift,
    dim_date
RESTART IDENTITY CASCADE;

INSERT INTO dim_donor (donor_id, donor_name)
SELECT
    donor_id,
    donor_name
FROM {{SOURCE_FDW_SCHEMA}}.mst_donor
ORDER BY donor_id;

INSERT INTO dim_program (
    program_id,
    program_name,
    donor_key,
    start_date,
    end_date,
    target_sessions,
    target_students
)
SELECT
    p.program_id,
    p.program_name,
    d.donor_key,
    p.start_date,
    p.end_date,
    p.target_sessions,
    p.target_students
FROM {{SOURCE_FDW_SCHEMA}}.mst_program p
LEFT JOIN dim_donor d ON d.donor_id = p.donor_id::varchar
ORDER BY p.program_id;

INSERT INTO dim_location (
    school_id,
    school_name,
    area,
    region,
    district,
    state,
    center_name
)
SELECT DISTINCT
    s.school_id::varchar,
    s.school_name,
    s.area,
    s.region,
    s.district,
    s.state,
    s.center_name
FROM {{SOURCE_FDW_SCHEMA}}.mst_school s
ORDER BY s.school_id::varchar;

INSERT INTO dim_instructor (
    instructor_id,
    name,
    instructor_type,
    region_assigned
)
SELECT
    i.instructor_id::varchar,
    i.instructor_name,
    i.instructor_type,
    i.assigned_region
FROM {{SOURCE_FDW_SCHEMA}}.mst_instructor i
ORDER BY i.instructor_id;

INSERT INTO dim_activity (
    activity_type_id,
    activity_name,
    activity_category
)
SELECT
    a.activity_id::varchar,
    a.activity_name,
    a.category
FROM {{SOURCE_FDW_SCHEMA}}.mst_activity_type a
ORDER BY a.activity_id;

INSERT INTO dim_shift (
    shift_key,
    shift_name
)
SELECT DISTINCT
    s.shift_id,
    s.shift_name
FROM {{SOURCE_FDW_SCHEMA}}.mst_shift s
ORDER BY s.shift_id;

WITH source_dates AS (
    SELECT session_date::date AS actual_date
    FROM {{SOURCE_FDW_SCHEMA}}.txn_session
    WHERE session_date IS NOT NULL

    UNION

    SELECT feedback_date::date AS actual_date
    FROM {{SOURCE_FDW_SCHEMA}}.mst_adhoc_session_feedback_answers
    WHERE feedback_date IS NOT NULL
),
date_bounds AS (
    SELECT DATE_TRUNC('month', MIN(actual_date))::date AS min_date, MAX(actual_date) AS max_date
    FROM source_dates
),
calendar AS (
    SELECT generate_series(min_date, max_date, interval '1 day')::date AS actual_date
    FROM date_bounds
    WHERE min_date IS NOT NULL
)
INSERT INTO dim_date (
    date_key,
    date,
    day,
    week,
    month,
    quarter,
    year,
    financial_year
)
SELECT
    TO_CHAR(actual_date, 'YYYYMMDD')::int AS date_key,
    actual_date AS date,
    EXTRACT(DAY FROM actual_date)::int AS day,
    EXTRACT(WEEK FROM actual_date)::int AS week,
    EXTRACT(MONTH FROM actual_date)::int AS month,
    EXTRACT(QUARTER FROM actual_date)::int AS quarter,
    EXTRACT(YEAR FROM actual_date)::int AS year,
    CASE
        WHEN EXTRACT(MONTH FROM actual_date) >= 4 THEN (EXTRACT(YEAR FROM actual_date)::int + 1)::varchar
        ELSE EXTRACT(YEAR FROM actual_date)::character varying
    END AS financial_year
FROM calendar
ORDER BY actual_date;

