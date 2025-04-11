SELECT locale_id, locality
FROM 
    {{ ref('stg_locale') }}