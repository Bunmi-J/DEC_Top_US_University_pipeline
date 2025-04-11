WITH required_test AS (
    SELECT DISTINCT test_requirements
    FROM {{ ref('stg_school_data') }}
),
test_status AS (
    SELECT test_requirements,
        CASE 
            WHEN test_requirements = 1 THEN 'Required'
            WHEN test_requirements = 3 THEN 'Not Required'
            WHEN test_requirements = 5 THEN 'Recommended'
            ELSE 'Not Specified'
        END AS test_options
    FROM required_test
)

SELECT *
FROM test_status
