{{ config( materialized="view" ) }}

WITH Us_state as (
    SELECT * 
    FROM {{ source('uni_ranking', 'STATE_FIDS') }}
),
code_state as (
    select DISTINCT state_fips, state_code
    from {{ ref('top_1000') }}
),
combine as (
    select state_fips_id, state, state_code
    from Us_state as u
    join code_state as c
    on u.state_fips_id = c.state_fips
)

select *
from combine
