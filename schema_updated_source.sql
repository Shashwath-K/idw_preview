-- ============================================================
-- SOURCE SCHEMA (ELT READY)
-- Purpose:
-- Operational + Staging schema for CSV / Frontend data load
-- Used as source for dbt ELT pipeline
-- ============================================================



-- ============================================================
-- DONOR
-- Stores donor information funding programs
-- Used for program tracking and analytics
-- ============================================================

CREATE TABLE mst_donor (

    donor_id SERIAL PRIMARY KEY,

    donor_name VARCHAR(255) NOT NULL,

    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);



-- ============================================================
-- PROGRAM
-- Program created under donor with targets
-- Used to measure achievement
-- ============================================================

CREATE TABLE mst_program (

    program_id SERIAL PRIMARY KEY,

    program_name VARCHAR(255) NOT NULL,

    donor_id INT,

    start_date DATE,
    end_date DATE,

    target_sessions INT,
    target_students INT,

    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);



-- ============================================================
-- SCHOOL / CENTER / LOCATION
-- Flattened location hierarchy for ELT simplicity
-- Region -> Area -> School -> Center
-- ============================================================

CREATE TABLE mst_school (

    school_id SERIAL PRIMARY KEY,

    school_name VARCHAR(255),

    region VARCHAR(100),
    area VARCHAR(100),
    district VARCHAR(100),
    state VARCHAR(100),

    center_name VARCHAR(255),

    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);



-- ============================================================
-- INSTRUCTOR / IGNITOR / VOLUNTEER
-- Person conducting the session
-- Stored separately for analytics
-- ============================================================

CREATE TABLE mst_instructor (

    instructor_id SERIAL PRIMARY KEY,

    instructor_name VARCHAR(255),

    instructor_type VARCHAR(100),

    assigned_region VARCHAR(100),

    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);



-- ============================================================
-- ACTIVITY TYPE
-- Used to determine JSON structure
-- Important for ELT parsing
-- ============================================================

CREATE TABLE mst_activity_type (

    activity_id SERIAL PRIMARY KEY,

    activity_name VARCHAR(255),

    category VARCHAR(100),

    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);



-- ============================================================
-- SHIFT
-- Morning / Afternoon / Evening etc
-- ============================================================

CREATE TABLE mst_shift (

    shift_id SERIAL PRIMARY KEY,

    shift_name VARCHAR(100),

    start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    end_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);



-- ============================================================
-- PROGRAM SCHOOL MAPPING
-- Mapping of program to school / instructor / shift
-- One mapping can have multiple sessions
-- ============================================================

CREATE TABLE conf_program_school_mapping (
    mapping_id SERIAL PRIMARY KEY,
    program_id INT,
    school_id INT,
    instructor_id INT,
    shift_id INT,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);



-- ============================================================
-- SESSION
-- Core transactional table
-- Each row = one conducted session
-- ============================================================

CREATE TABLE txn_session (
    session_id SERIAL PRIMARY KEY,
    mapping_id INT,
    session_date DATE,
    activity_id INT,
    instructor_id INT,   -- session-level instructor (ELT friendly)
    status VARCHAR(50),
    updated_at TIMESTAMP,
    is_processed BOOLEAN DEFAULT FALSE,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);



-- ============================================================
-- FEEDBACK ANSWER
-- ============================================================

CREATE TABLE txn_feedback_answer (
    feedback_id SERIAL PRIMARY KEY,
    session_id INT,
    question_id INT,
    question_asked VARCHAR(100),
    question_answer VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);



-- ============================================================
-- FEEDBACK EXPOSURE
-- Numeric outreach values extracted from feedback
-- Used for analytics
-- ============================================================

CREATE TABLE txn_feedback_exposure (

    exposure_id SERIAL PRIMARY KEY,

    feedback_id INT,

    boys INT DEFAULT 0,
    girls INT DEFAULT 0,

    teachers INT DEFAULT 0,

    community_men INT DEFAULT 0,
    community_women INT DEFAULT 0,

    guests INT DEFAULT 0,
    total_students INT DEFAULT 0,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);



-- ============================================================
-- ADHOC SESSION FEEDBACK
-- Separate flow for adhoc events
-- Similar to regular but single table
-- Used for ELT union later
-- ============================================================

CREATE TABLE mst_adhoc_session_feedback_answers (

    id SERIAL PRIMARY KEY,
    program_id INT,
    instructor_id INT,
    activity_id INT,
    school_id INT,
    feedback_date DATE,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);



-- ============================================================
-- INDEXES (important for ELT performance)
-- ============================================================

CREATE INDEX idx_session_mapping
ON txn_session(mapping_id);

CREATE INDEX idx_session_date
ON txn_session(session_date);

CREATE INDEX idx_feedback_session
ON txn_feedback_answer(session_id);

CREATE INDEX idx_exposure_feedback
ON txn_feedback_exposure(feedback_id);

CREATE INDEX idx_mapping_program
ON conf_program_school_mapping(program_id);

CREATE INDEX idx_mapping_school
ON conf_program_school_mapping(school_id);

CREATE INDEX idx_mapping_instructor
ON conf_program_school_mapping(instructor_id);

CREATE INDEX idx_activity
ON txn_session(activity_id);