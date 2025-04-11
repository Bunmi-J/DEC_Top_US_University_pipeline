SELECT locale_id, locale as locality
FROM 
    {{ source('uni_ranking', 'locale') }}
