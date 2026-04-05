-- [DW] dw_schema_change_v4.sql
-- Description: Star Schema definitions in the 'dw' schema.
-- Includes Dimensions and Measured Fact tables with surrogate keys.

SET search_path TO dw;

--------------------------------------------------------------------------------
-- 1. DIMENSIONS
--------------------------------------------------------------------------------

-- DIM_GEOGRAPHY (Area + Region Hierarchy)
CREATE TABLE IF NOT EXISTS dw.dim_geography (
    sk_geography_id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    nk_area_id INTEGER,
    nk_region_id INTEGER,
    area_name VARCHAR(100),
    region_name VARCHAR(100),
    area_code VARCHAR(50),
    region_code VARCHAR(50),
    is_deleted BOOLEAN DEFAULT FALSE,
    effective_date TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- DIM_USER (Instructors, Managers, Admins)
CREATE TABLE IF NOT EXISTS dw.dim_user (
    sk_user_id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    nk_user_id INTEGER UNIQUE,
    user_name VARCHAR(100) NOT NULL,
    user_code VARCHAR(50),
    email VARCHAR(100),
    role_name VARCHAR(100),
    manager_name VARCHAR(100),
    joining_date DATE,
    has_b_ed BOOLEAN,
    has_d_ed BOOLEAN,
    pg_degree VARCHAR(50),
    ug_degree VARCHAR(50),
    is_active BOOLEAN DEFAULT TRUE,
    effective_date TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- DIM_SCHOOL
CREATE TABLE IF NOT EXISTS dw.dim_school (
    sk_school_id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    nk_school_id INTEGER UNIQUE,
    school_name VARCHAR(100) NOT NULL,
    school_code VARCHAR(50),
    udise_code VARCHAR(20),
    address VARCHAR(300),
    pincode VARCHAR(20),
    school_type_name VARCHAR(100),
    school_category_id INTEGER,
    state_management_id INTEGER,
    is_deleted BOOLEAN DEFAULT FALSE,
    effective_date TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- DIM_PROGRAM
CREATE TABLE IF NOT EXISTS dw.dim_program (
    sk_program_id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    nk_program_id INTEGER UNIQUE,
    program_name VARCHAR(150) NOT NULL,
    donor_name VARCHAR(100),
    donor_code VARCHAR(50),
    start_date DATE,
    end_date DATE,
    instructor_capacity INTEGER,
    periodicity VARCHAR(50),
    is_deleted BOOLEAN DEFAULT FALSE,
    effective_date TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- DIM_ACTIVITY_TYPE
CREATE TABLE IF NOT EXISTS dw.dim_activity_type (
    sk_activity_type_id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    nk_activity_type_id INTEGER UNIQUE,
    activity_code VARCHAR(255),
    activity_name VARCHAR(255),
    is_adhoc BOOLEAN,
    effective_date TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- DIM_SUBJECT_TOPIC
CREATE TABLE IF NOT EXISTS dw.dim_subject_topic (
    sk_subject_topic_id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    nk_topic_id INTEGER UNIQUE,
    topic_description VARCHAR(300),
    subject_name VARCHAR(100),
    subject_code VARCHAR(50),
    effective_date TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- DIM_DATE
CREATE TABLE IF NOT EXISTS dw.dim_date (
    date_id INTEGER PRIMARY KEY, -- YYYYMMDD
    full_date DATE NOT NULL,
    day_of_week INTEGER,
    day_name VARCHAR(10),
    day_of_month INTEGER,
    month_name VARCHAR(10),
    month_actual INTEGER,
    quarter_actual INTEGER,
    year_actual INTEGER,
    is_weekend BOOLEAN
);

--------------------------------------------------------------------------------
-- 2. FACT TABLES
--------------------------------------------------------------------------------

-- FACT_SESSION (Measures from TXN_SESSION & RPT_FEEDBACK)
CREATE TABLE IF NOT EXISTS dw.fact_session (
    sk_fact_session_id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    date_id INTEGER REFERENCES dw.dim_date(date_id),
    sk_user_id BIGINT REFERENCES dw.dim_user(sk_user_id),
    sk_school_id BIGINT REFERENCES dw.dim_school(sk_school_id),
    sk_program_id BIGINT REFERENCES dw.dim_program(sk_program_id),
    sk_activity_type_id BIGINT REFERENCES dw.dim_activity_type(sk_activity_type_id),
    sk_subject_topic_id BIGINT REFERENCES dw.dim_subject_topic(sk_subject_topic_id),
    sk_geography_id BIGINT REFERENCES dw.dim_geography(sk_geography_id),
    
    session_nk_id INTEGER,
    demo_session_count INTEGER DEFAULT 0,
    hands_on_session_count INTEGER DEFAULT 0,
    session_duration_minutes DECIMAL(10,2),
    no_of_teachers_participated INTEGER,
    no_of_models_displayed INTEGER,
    community_men_count INTEGER DEFAULT 0,
    community_women_count INTEGER DEFAULT 0,
    is_overdue BOOLEAN,
    
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);


-- FACT_ATTENDANCE_EXPOSURE (Detailed metrics from TXN_FEEDBACK_EXPOSURE)
CREATE TABLE IF NOT EXISTS dw.fact_attendance_exposure (
    sk_fact_attendance_id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    date_id INTEGER REFERENCES dw.dim_date(date_id),
    sk_user_id BIGINT REFERENCES dw.dim_user(sk_user_id),
    sk_school_id BIGINT REFERENCES dw.dim_school(sk_school_id),
    sk_program_id BIGINT REFERENCES dw.dim_program(sk_program_id),
    sk_geography_id BIGINT REFERENCES dw.dim_geography(sk_geography_id),
    
    session_nk_id INTEGER,
    class_name VARCHAR(100),
    section_name VARCHAR(50),
    boys_count INTEGER DEFAULT 0,
    girls_count INTEGER DEFAULT 0,
    total_exposure_count INTEGER DEFAULT 0,
    
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- FACT_VEHICLE_OPERATIONS (Metrics from TXN_VEHICLE_LOG)
CREATE TABLE IF NOT EXISTS dw.fact_vehicle_operations (
    sk_fact_vehicle_id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    date_id INTEGER REFERENCES dw.dim_date(date_id),
    sk_user_id BIGINT REFERENCES dw.dim_user(sk_user_id), -- Driver or Instructor
    sk_program_id BIGINT REFERENCES dw.dim_program(sk_program_id),
    sk_geography_id BIGINT REFERENCES dw.dim_geography(sk_geography_id),
    
    vehicle_nk_id INTEGER,
    distance_travelled INTEGER, -- closed_reading - open_reading
    fuel_quantity FLOAT,
    fuel_cost FLOAT,
    was_vehicle_used BOOLEAN,
    
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

--------------------------------------------------------------------------------
-- 3. INDEXES FOR PERFORMANCE
--------------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_fact_session_date ON dw.fact_session(date_id);
CREATE INDEX IF NOT EXISTS idx_fact_attendance_date ON dw.fact_attendance_exposure(date_id);
CREATE INDEX IF NOT EXISTS idx_fact_session_user ON dw.fact_session(sk_user_id);
CREATE INDEX IF NOT EXISTS idx_fact_session_school ON dw.fact_session(sk_school_id);
