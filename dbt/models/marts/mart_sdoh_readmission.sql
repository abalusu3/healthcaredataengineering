-- Readmissions by member county (FIPS) joined to CDC Social Vulnerability Index.
-- County name/state come from the SVI file keyed by FIPS: Synthea's free-text county can
-- disagree with its FIPS code, and FIPS is the authoritative join key.
select
    m.county_fips,
    s.county,
    s.state_code,
    s.svi_overall,
    s.svi_quartile,
    s.population as county_population,
    count(distinct f.member_key) as members_admitted,
    count(*) as index_admissions,
    sum(f.readmit_30d::int) as readmissions,
    round(avg(f.readmit_30d::int)::numeric, 4) as readmit_rate,
    round(sum(f.readmit_cost), 2) as readmission_cost,
    round(avg(f.lace_score)::numeric, 2) as avg_lace_score
from {{ ref('fct_admissions') }} f
join {{ ref('dim_member') }} m using (member_key)
left join {{ ref('stg_svi_county') }} s on s.county_fips = m.county_fips
where f.is_index_eligible
group by 1, 2, 3, 4, 5, 6
