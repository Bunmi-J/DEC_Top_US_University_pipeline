WITH ownership_status AS (
    SELECT DISTINCT ownership_peps
    FROM {{ ref('stg_school_data') }}
),
status_name AS (
    SELECT ownership_peps,
        CASE 
            WHEN ownership_peps = 1 THEN 'Public'
            WHEN ownership_peps = 2 THEN 'Private Nonprofit'
            ELSE 'Private For-Profit'
        END AS ownership_purpose
    FROM ownership_status
)

SELECT ownership_peps, ownership_purpose
FROM status_name
