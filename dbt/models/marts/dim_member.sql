select
    {{ dbt_utils.generate_surrogate_key(['m.member_id']) }} as member_key,
    m.member_id,
    m.birth_date,
    m.death_date,
    m.gender,
    m.race,
    m.ethnicity,
    m.member_state,
    m.source_state,
    m.county,
    m.county_fips,
    m.zip,
    m.income,
    s.svi_overall,
    s.svi_quartile
from {{ ref('stg_members') }} m
left join {{ ref('stg_svi_county') }} s using (county_fips)
