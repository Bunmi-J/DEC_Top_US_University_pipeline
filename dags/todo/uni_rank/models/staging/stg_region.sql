SELECT *
FROM 
    {{ source('uni_ranking', 'region') }}
