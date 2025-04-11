SELECT carnegie_basic_id,
    carnegie_basic_classification
FROM 
    {{ ref('stg_carnegie_basic_dict') }}