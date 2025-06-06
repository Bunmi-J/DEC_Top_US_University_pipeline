CREATE TABLE IF NOT EXISTS school_data (
    rank INT,
    id INT PRIMARY KEY,
    school_name VARCHAR(255),
    address VARCHAR(255),
    city VARCHAR(100),
    state_code VARCHAR(50),
    state_fips INT,
    region_id INT,
    school_locale INT,
    school_url VARCHAR(255),
    school_ownership INT,
    school_accreditor VARCHAR(255),
    price_calculator_url VARCHAR(255),
    online_only INT,
    main_campus boolean,
    institutional_characteristics_level INT,
    open_admissions_policy INT,
    degrees_awarded_highest INT,
    degrees_awarded_predominant INT,
    booksupply_cost FLOAT,
    test_requirements FLOAT,
    pell_grant_rate FLOAT,
    sat_scores_overall_average INT,
    carnegie_basic INT,
    act_scores_50th_percentile INT,
    admission_rate_overall FLOAT,
    completion_rate_suppressed FLOAT,
    retention_rate_full_time FLOAT,
    federal_loan_rate FLOAT,
    cost_of_attendance FLOAT,
    avg_net_price_overall FLOAT,
    tuition_in_state FLOAT,
    tuition_out_of_state FLOAT,
    roomboard_offcampus FLOAT,
    roomboard_oncampus FLOAT,
    otherexpense_oncampus FLOAT,
    otherexpense_offcampus FLOAT,
    student_size INT,
    student_demographics_men FLOAT,
    student_demographics_women FLOAT,
    faculty_demographics_men FLOAT,
    faculty_demographics_women FLOAT,
    grad_enrollment_12_month INT,
    undergrad_enrollment_12_month INT
);
