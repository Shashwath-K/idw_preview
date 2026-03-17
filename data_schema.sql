-- ============================================
-- DATA MART SCHEMA (FACT + DIM)
-- PostgreSQL
-- ============================================


-- ============================
-- DIM_DATE
-- ============================

CREATE TABLE dim_date (
    date_key INT PRIMARY KEY,
    date_value DATE NOT NULL,
    day_name VARCHAR(9),
    week INT,
    month INT,
    month_name VARCHAR(10),
    quarter INT,
    year INT,
    financial_year INT
);


-- ============================
-- DIM_DONOR
-- ============================

CREATE TABLE dim_donor (
    donor_key SERIAL PRIMARY KEY,
    donor_id INT,
    donor_name VARCHAR(255)
);


-- ============================
-- DIM_PROGRAM
-- ============================

CREATE TABLE dim_program (
    program_key SERIAL PRIMARY KEY,
    program_id INT,
    program_name VARCHAR(255),
    donor_key INT,
    start_date DATE,
    end_date DATE,
    target_sessions INT,
    target_students INT,

    FOREIGN KEY (donor_key)
    REFERENCES dim_donor(donor_key)
);


-- ============================
-- DIM_LOCATION
-- ============================

CREATE TABLE dim_location (
    location_key SERIAL PRIMARY KEY,
    school_id INT,
    school_name VARCHAR(255),
    area VARCHAR(100),
    region VARCHAR(100),
    district VARCHAR(100),
    state VARCHAR(100),
    center_name VARCHAR(255),

    UNIQUE(region, area, district, school_name, center_name)
);


-- ============================
-- DIM_INSTRUCTOR
-- ============================

CREATE TABLE dim_instructor (
    instructor_key SERIAL PRIMARY KEY,
    instructor_id INT,
    instructor_name VARCHAR(255),
    instructor_type VARCHAR(100),
    assigned_region VARCHAR(100)
);


-- ============================
-- DIM_ACTIVITY
-- ============================

CREATE TABLE dim_activity (
    activity_key SERIAL PRIMARY KEY,
    activity_id INT,
    activity_name VARCHAR(255),
    category VARCHAR(100)
);


-- ============================
-- DIM_SHIFT
-- ============================

CREATE TABLE dim_shift (
    shift_key SERIAL PRIMARY KEY,
    shift_id INT,
    shift_name VARCHAR(100)
);


-- ============================
-- FACT_SESSION_EVENT
-- grain = 1 row per session
-- ============================

CREATE TABLE fact_session_event (

    session_key SERIAL PRIMARY KEY,

    session_id INT,

    date_key INT,
    location_key INT,
    program_key INT,
    activity_key INT,
    instructor_key INT,
    shift_key INT,

    session_duration INT,
    engagement_mode VARCHAR(50),
    is_overdue BOOLEAN,

    session_count INT DEFAULT 1,

    FOREIGN KEY (date_key)
        REFERENCES dim_date(date_key),

    FOREIGN KEY (location_key)
        REFERENCES dim_location(location_key),

    FOREIGN KEY (program_key)
        REFERENCES dim_program(program_key),

    FOREIGN KEY (activity_key)
        REFERENCES dim_activity(activity_key),

    FOREIGN KEY (instructor_key)
        REFERENCES dim_instructor(instructor_key),

    FOREIGN KEY (shift_key)
        REFERENCES dim_shift(shift_key)
);


-- ============================
-- FACT_EXPOSURE
-- grain = exposure per session
-- ============================

CREATE TABLE fact_exposure (

    exposure_key SERIAL PRIMARY KEY,

    session_key INT,

    boys_count INT,
    girls_count INT,
    students_total INT,
    teachers_count INT,
    community_men INT,
    community_women INT,
    guests_count INT,

    FOREIGN KEY (session_key)
        REFERENCES fact_session_event(session_key)
);


-- ============================
-- FACT_SESSION_ATTRIBUTE
-- dynamic attributes / JSON extraction
-- ============================

CREATE TABLE fact_session_attribute (

    attribute_key SERIAL PRIMARY KEY,

    session_key INT,
    attribute_name VARCHAR(100),
    attribute_value TEXT,

    FOREIGN KEY (session_key)
        REFERENCES fact_session_event(session_key)
);


-- ============================
-- FACT_MONTHLY_REGION_IMPACT
-- ============================

CREATE TABLE fact_monthly_region_impact (

    impact_key SERIAL PRIMARY KEY,

    date_key INT,
    location_key INT,

    sessions_count INT,
    students_reached INT,
    boys_count INT,
    girls_count INT,
    teachers_count INT,

    FOREIGN KEY (date_key)
        REFERENCES dim_date(date_key),

    FOREIGN KEY (location_key)
        REFERENCES dim_location(location_key)
);


-- ============================
-- FACT_WEEKLY_PROGRAM_METRICS
-- ============================

CREATE TABLE fact_weekly_program_metrics (

    metric_key SERIAL PRIMARY KEY,

    date_key INT,
    program_key INT,

    sessions_count INT,
    students_reached INT,
    target_sessions INT,
    target_students INT,

    FOREIGN KEY (date_key)
        REFERENCES dim_date(date_key),

    FOREIGN KEY (program_key)
        REFERENCES dim_program(program_key)
);


-- ============================
-- FACT_INSTRUCTOR_PRODUCTIVITY
-- ============================

CREATE TABLE fact_instructor_productivity (

    productivity_key SERIAL PRIMARY KEY,

    date_key INT,
    instructor_key INT,

    sessions_conducted INT,
    students_reached INT,
    avg_session_duration NUMERIC,
    overdue_sessions INT,

    FOREIGN KEY (date_key)
        REFERENCES dim_date(date_key),

    FOREIGN KEY (instructor_key)
        REFERENCES dim_instructor(instructor_key)
);


-- ============================================
-- INDEXES (Recommended)
-- ============================================

CREATE INDEX idx_fact_session_date
ON fact_session_event(date_key);

CREATE INDEX idx_fact_session_location
ON fact_session_event(location_key);

CREATE INDEX idx_fact_session_program
ON fact_session_event(program_key);

CREATE INDEX idx_fact_exposure_session
ON fact_exposure(session_key);

CREATE INDEX idx_fact_region_date
ON fact_monthly_region_impact(date_key);

CREATE INDEX idx_fact_program_date
ON fact_weekly_program_metrics(date_key);

CREATE INDEX idx_fact_instructor_date
ON fact_instructor_productivity(date_key);