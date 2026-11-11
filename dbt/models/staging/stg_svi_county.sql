select
    county_fips,
    state_code,
    county,
    population,
    svi_overall,
    svi_socioeconomic,
    svi_household,
    svi_minority_language,
    svi_housing_transport,
    case
        when svi_overall is null then null
        when svi_overall < 0.25 then 'Q1 (least vulnerable)'
        when svi_overall < 0.50 then 'Q2'
        when svi_overall < 0.75 then 'Q3'
        else 'Q4 (most vulnerable)'
    end as svi_quartile
from {{ source('raw', 'svi_county') }}
