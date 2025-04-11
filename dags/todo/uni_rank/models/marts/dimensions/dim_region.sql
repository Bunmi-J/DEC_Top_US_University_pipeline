SELECT region_id, region
FROM 
    {{ ref('stg_region') }}