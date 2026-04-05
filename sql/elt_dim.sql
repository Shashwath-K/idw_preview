-- [ELT] elt_dim.sql
-- Description: Dimension loading logic (SCD Type 1).
-- Includes Date Dimension generation and transformation logic.

SET search_path TO dw, source;

--------------------------------------------------------------------------------
-- 1. DIM_DATE (Calendar Generation)
--------------------------------------------------------------------------------
TRUNCATE TABLE dw.dim_date CASCADE;
INSERT INTO dw.dim_date (date_id, full_date, day_of_week, day_name, day_of_month, month_name, month_actual, quarter_actual, year_actual, is_weekend)
SELECT 
    CAST(TO_CHAR(datum, 'YYYYMMDD') AS INT) AS date_id,
    datum AS full_date,
    EXTRACT(DOW FROM datum) + 1 AS day_of_week,
    TO_CHAR(datum, 'Day') AS day_name,
    EXTRACT(DAY FROM datum) AS day_of_month,
    TO_CHAR(datum, 'Month') AS month_name,
    EXTRACT(MONTH FROM datum) AS month_actual,
    EXTRACT(QUARTER FROM datum) AS quarter_actual,
    EXTRACT(YEAR FROM datum) AS year_actual,
    CASE WHEN EXTRACT(DOW FROM datum) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend
FROM generate_series('2010-01-01'::DATE, '2030-12-31'::DATE, '1 day'::INTERVAL) AS datum;

--------------------------------------------------------------------------------
-- 2. DIM_GEOGRAPHY (Area + Region)
--------------------------------------------------------------------------------
INSERT INTO dw.dim_geography (nk_area_id, nk_region_id, area_name, region_name, area_code, region_code, is_deleted)
SELECT 
    a.id as nk_area_id,
    r.id as nk_region_id,
    a.name as area_name,
    r.name as region_name,
    a.code as area_code,
    r.code as region_code,
    COALESCE(a.is_deleted::BOOLEAN, false) OR COALESCE(r.is_deleted::BOOLEAN, false)
FROM source.mst_area a
JOIN source.mst_region r ON a.region_id = r.id;

--------------------------------------------------------------------------------
-- 3. DIM_USER (SCD Type 1)
--------------------------------------------------------------------------------
INSERT INTO dw.dim_user (nk_user_id, user_name, user_code, email, role_name, manager_name, joining_date, has_b_ed, has_d_ed, pg_degree, ug_degree, is_active)
SELECT 
    u.id as nk_user_id,
    u.name as user_name,
    u.code as user_code,
    u.email,
    r.name as role_name,
    m.name as manager_name,
    u.joining_date,
    u.has_b_ed_degree::BOOLEAN,
    u.has_d_ed_degree::BOOLEAN,
    u.pg_degree,
    u.ug_degree,
    u.is_deleted = 0 as is_active

FROM source.mst_user u
LEFT JOIN source.mst_role r ON u.role_id = r.id
LEFT JOIN source.mst_user m ON u.report_id = m.id;

--------------------------------------------------------------------------------
-- 4. DIM_SCHOOL
--------------------------------------------------------------------------------
INSERT INTO dw.dim_school (nk_school_id, school_name, school_code, udise_code, address, pincode, school_type_name, school_category_id, state_management_id, is_deleted)
SELECT 
    s.id as nk_school_id,
    s.name as school_name,
    s.code as school_code,
    s.udise_code,
    s.address,
    s.pincode,
    st.name as school_type_name,
    s.school_category as school_category_id,
    s.state_management as state_management_id,
    s.is_deleted::BOOLEAN
FROM source.mst_school s
LEFT JOIN source.mst_school_type st ON s.school_type = st.id;

--------------------------------------------------------------------------------
-- 5. DIM_PROGRAM
--------------------------------------------------------------------------------
INSERT INTO dw.dim_program (nk_program_id, program_name, donor_name, donor_code, start_date, end_date, instructor_capacity, periodicity, is_deleted)
SELECT 
    p.id as nk_program_id,
    p.name as program_name,
    d.name as donor_name,
    d.code as donor_code,
    p.start_date,
    p.end_date,
    p.instructor_capacity,
    CAST(p.periodicity_id AS TEXT), 
    p.is_deleted = 1

FROM source.txn_program p
LEFT JOIN source.mst_donor d ON p.donor_id = d.id;

--------------------------------------------------------------------------------
-- 6. DIM_ACTIVITY_TYPE
--------------------------------------------------------------------------------
INSERT INTO dw.dim_activity_type (nk_activity_type_id, activity_code, activity_name, is_adhoc)
SELECT 
    id as nk_activity_type_id,
    code as activity_code,
    name as activity_name,
    is_adhoc::BOOLEAN
FROM source.mst_activity_type;

--------------------------------------------------------------------------------
-- 7. DIM_SUBJECT_TOPIC
--------------------------------------------------------------------------------
INSERT INTO dw.dim_subject_topic (nk_topic_id, topic_description, subject_name, subject_code)
SELECT 
    t.id as nk_topic_id,
    t.description as topic_description,
    s.name as subject_name,
    s.code as subject_code
FROM source.mst_topic t
LEFT JOIN source.mst_subject s ON t.subject_id = s.id;
