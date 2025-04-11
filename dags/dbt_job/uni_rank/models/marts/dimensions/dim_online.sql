WITH mode_online AS (
    SELECT DISTINCT ONLINE_ONLY
    FROM {{ ref('stg_school_data') }}
),
mode_name AS (
    SELECT ONLINE_ONLY,
        CASE 
            WHEN ONLINE_ONLY = 1 THEN 'Distance-education only'
            WHEN ONLINE_ONLY = 0 THEN 'Not distance-education only'
            ELSE 'Not Specified'
        END AS mode
    FROM mode_online
)

SELECT *
FROM mode_name
