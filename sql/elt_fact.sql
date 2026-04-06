-- [ELT] elt_fact.sql
-- Description: Fact table loading logic.
-- Populates measures and lookups surrogate keys from dimensions.

SET search_path TO dw, source;

--------------------------------------------------------------------------------
-- 1. FACT_SESSION (Measures from TXN_SESSION + RPT_FEEDBACK)
--------------------------------------------------------------------------------
TRUNCATE TABLE dw.fact_session CASCADE;
INSERT INTO dw.fact_session (
    date_id, sk_user_id, sk_school_id, sk_program_id, sk_activity_type_id, 
    sk_subject_topic_id, sk_geography_id, session_nk_id, 
    demo_session_count, hands_on_session_count, session_duration_minutes, 
    no_of_teachers_participated, no_of_models_displayed, 
    community_men_count, community_women_count, is_overdue

)
SELECT 
    d.date_id, 
    u.sk_user_id, 
    s.sk_school_id, 
    p.sk_program_id, 
    at.sk_activity_type_id,
    st.sk_subject_topic_id,
    g.sk_geography_id,
    ts.id as session_nk_id,
    COALESCE(rf.demo_session, 0) as demo_session_count,
    COALESCE(rf.hands_on_session, 0) as hands_on_session_count,
    COALESCE(rf.session_duration, 0) as session_duration_minutes,
    COALESCE(rf.no_of_teachers, 0) as no_of_teachers_participated,
    COALESCE(rf.no_of_model_displayed, 0) as no_of_models_displayed,
    COALESCE(rf.no_of_men, 0) as community_men_count,
    COALESCE(rf.no_of_women, 0) as community_women_count,
    ts.is_overdue = 1

FROM source.txn_session ts
-- Lookups
LEFT JOIN dw.dim_date d ON ts.date = d.full_date
LEFT JOIN dw.dim_user u ON ts.instructor_id = u.nk_user_id
LEFT JOIN source.conf_program_school_mapping cpsm ON ts.program_school_mapped_id = cpsm.id
LEFT JOIN dw.dim_school s ON cpsm.school_id = s.nk_school_id
LEFT JOIN dw.dim_program p ON cpsm.program_id = p.nk_program_id
LEFT JOIN dw.dim_activity_type at ON cpsm.activity_type_id = at.nk_activity_type_id
LEFT JOIN source.rpt_feedback rf ON ts.id = rf.session_id
LEFT JOIN dw.dim_subject_topic st ON rf.topic_id = st.nk_topic_id
LEFT JOIN source.mst_school ms ON cpsm.school_id = ms.id
LEFT JOIN dw.dim_geography g ON ms.area_id = g.nk_area_id;

--------------------------------------------------------------------------------
-- 2. FACT_ATTENDANCE_EXPOSURE (Detailed metrics from TXN_FEEDBACK_EXPOSURE)
--------------------------------------------------------------------------------
TRUNCATE TABLE dw.fact_attendance_exposure CASCADE;
INSERT INTO dw.fact_attendance_exposure (
    date_id, sk_user_id, sk_school_id, sk_program_id, sk_geography_id, 
    session_nk_id, class_name, section_name, boys_count, girls_count, total_exposure_count
)
SELECT 
    d.date_id,
    u.sk_user_id,
    s.sk_school_id,
    p.sk_program_id,
    g.sk_geography_id,
    tfe.session_id as session_nk_id,
    mc.name as class_name,
    tfe.section as section_name,
    COALESCE(tfe.boys, 0) as boys_count,
    COALESCE(tfe.girls, 0) as girls_count,
    COALESCE(tfe.boys, 0) + COALESCE(tfe.girls, 0) as total_exposure_count
FROM source.txn_feedback_exposure tfe
JOIN source.txn_session ts ON tfe.session_id = ts.id
LEFT JOIN dw.dim_date d ON ts.date = d.full_date
LEFT JOIN dw.dim_user u ON ts.instructor_id = u.nk_user_id
LEFT JOIN source.conf_program_school_mapping cpsm ON ts.program_school_mapped_id = cpsm.id
LEFT JOIN dw.dim_school s ON cpsm.school_id = s.nk_school_id
LEFT JOIN dw.dim_program p ON cpsm.program_id = p.nk_program_id
LEFT JOIN source.mst_school ms ON cpsm.school_id = ms.id
LEFT JOIN dw.dim_geography g ON ms.area_id = g.nk_area_id
LEFT JOIN source.mst_class mc ON tfe.class_id = mc.id;

--------------------------------------------------------------------------------
-- 3. FACT_VEHICLE_OPERATIONS (Metrics from TXN_VEHICLE_LOG)
--------------------------------------------------------------------------------
TRUNCATE TABLE dw.fact_vehicle_operations CASCADE;
INSERT INTO dw.fact_vehicle_operations (
    date_id, sk_user_id, sk_instructor_id, sk_driver_id, sk_program_id, sk_geography_id, 
    vehicle_nk_id, distance_travelled, fuel_quantity, fuel_cost, was_vehicle_used
)
SELECT 
    d.date_id,
    ui.sk_user_id as sk_user_id,
    ui.sk_user_id as sk_instructor_id,
    ud.sk_user_id as sk_driver_id,
    p.sk_program_id,
    g.sk_geography_id,
    tvl.vehicle_id as vehicle_nk_id,
    COALESCE(tvl.closed_reading - tvl.open_reading, 0) as distance_travelled,
    COALESCE(tvl.fuel_quantity, 0),
    COALESCE(tvl.fuel_quantity * tvl.fuel_price, 0) as fuel_cost,
    tvl.vehicle_used_flag = 1
FROM source.txn_vehicle_log tvl
LEFT JOIN dw.dim_date d ON tvl.date = d.full_date
LEFT JOIN dw.dim_user ui ON tvl.instructor_id = ui.nk_user_id
LEFT JOIN dw.dim_user ud ON tvl.driver_id = ud.nk_user_id
LEFT JOIN dw.dim_program p ON tvl.program_id = p.nk_program_id
LEFT JOIN source.txn_program sp ON tvl.program_id = sp.id
LEFT JOIN dw.dim_geography g ON sp.area_id = g.nk_area_id;
