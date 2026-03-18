WITH regular_sessions AS (
    SELECT
        s.session_id,
        s.session_date,
        m.school_id,
        m.program_id,
        s.activity_id,
        COALESCE(s.instructor_id, m.instructor_id) AS instructor_id,
        m.shift_id,
        s.status
    FROM {{SOURCE_FDW_SCHEMA}}.txn_session s
    LEFT JOIN {{SOURCE_FDW_SCHEMA}}.conf_program_school_mapping m
        ON m.mapping_id = s.mapping_id
),
adhoc_sessions AS (
    SELECT
        -1 * a.id AS session_id,
        a.feedback_date AS session_date,
        a.school_id,
        a.program_id,
        a.activity_id,
        a.instructor_id,
        NULL::int AS shift_id,
        'adhoc'::varchar AS status
    FROM {{SOURCE_FDW_SCHEMA}}.mst_adhoc_session_feedback_answers a
),
all_sessions AS (
    SELECT * FROM regular_sessions
    UNION ALL
    SELECT * FROM adhoc_sessions
)
INSERT INTO fact_session_event (
    session_key,
    date_key,
    location_key,
    program_key,
    activity_key,
    instructor_key,
    shift_key,
    session_duration,
    engagement_mode,
    is_overdue,
    session_count
)
SELECT
    s.session_id,
    d.date_key,
    l.location_key,
    p.program_key,
    a.activity_key,
    i.instructor_key,
    sh.shift_key,
    1 AS session_duration,
    'Offline' AS engagement_mode,
    COALESCE(LOWER(s.status), '') IN ('overdue', 'late', 'delayed') AS is_overdue,
    1 AS session_count
FROM all_sessions s
LEFT JOIN dim_date d ON d.date = s.session_date
LEFT JOIN dim_location l ON l.school_id = s.school_id::varchar
LEFT JOIN dim_program p ON p.program_id = s.program_id::varchar
LEFT JOIN dim_activity a ON a.activity_type_id = s.activity_id::varchar
LEFT JOIN dim_instructor i ON i.instructor_id = s.instructor_id::varchar
LEFT JOIN dim_shift sh ON sh.shift_key = s.shift_id;

WITH exposure_by_session AS (
    SELECT
        fa.session_id,
        SUM(COALESCE(fe.boys, 0)) AS boys_count,
        SUM(COALESCE(fe.girls, 0)) AS girls_count,
        SUM(COALESCE(fe.total_students, 0)) AS students_total,
        SUM(COALESCE(fe.teachers, 0)) AS teachers_count,
        SUM(COALESCE(fe.community_men, 0)) AS community_men,
        SUM(COALESCE(fe.community_women, 0)) AS community_women,
        SUM(COALESCE(fe.guests, 0)) AS guests_count
    FROM {{SOURCE_FDW_SCHEMA}}.txn_feedback_exposure fe
    JOIN {{SOURCE_FDW_SCHEMA}}.txn_feedback_answer fa
        ON fa.feedback_id = fe.feedback_id
    GROUP BY fa.session_id
)
INSERT INTO fact_exposure (
    session_key,
    boys_count,
    girls_count,
    students_total,
    teachers_count,
    community_men,
    community_women,
    guests_count
)
SELECT
    f.session_key,
    e.boys_count,
    e.girls_count,
    e.students_total,
    e.teachers_count,
    e.community_men,
    e.community_women,
    e.guests_count
FROM exposure_by_session e
JOIN fact_session_event f ON f.session_key = e.session_id;

INSERT INTO fact_session_attribute (
    session_key,
    attribute_name,
    attribute_value
)
SELECT
    f.session_key,
    COALESCE(NULLIF(a.question_called, ''), CONCAT('question_', a.question_id::text)) AS attribute_name,
    a.question_answer AS attribute_value
FROM {{SOURCE_FDW_SCHEMA}}.txn_feedback_answer a
JOIN fact_session_event f ON f.session_key = a.session_id;
