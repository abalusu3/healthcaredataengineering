select
    member_id,
    birth_date,
    death_date,
    gender,
    race,
    ethnicity,
    marital_status,
    city,
    member_state,
    county,
    county_fips,
    zip,
    income,
    source_state
from {{ source('raw', 'members') }}
