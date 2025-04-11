WITH institution AS (
    SELECT DISTINCT institution_level
    FROM {{ ref('stg_school_data') }}
),
institute_level AS (
    SELECT institution_level,
        CASE 
            WHEN institution_level = 1 THEN '4-year'
            WHEN institution_level = 2 THEN '2-year'
            ELSE 'Less than 2-year'
        END AS level_name
    FROM institution
)

SELECT *
FROM institute_level
