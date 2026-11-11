-- Network scorecard: observed vs risk-expected readmissions per hospital.
-- Expected = sum over the hospital's index admissions of the plan-wide readmission rate for
-- that admission's LACE risk tier (indirect standardisation), so hospitals treating sicker
-- members are not penalised for case mix.
with idx as (
    select * from {{ ref('fct_admissions') }} where is_index_eligible
),

tier_rates as (
    select risk_tier, avg(readmit_30d::int)::numeric as tier_rate
    from idx group by 1
),

plan as (
    select avg(readmit_30d::int)::numeric as plan_rate from idx
),

by_hospital as (
    select
        i.hospital_key,
        count(*) as index_admissions,
        sum(i.readmit_30d::int) as readmissions,
        sum(t.tier_rate) as expected_readmissions,
        sum(i.readmit_cost) as readmission_cost,
        sum(i.total_claim_cost) as index_admission_cost,
        avg(i.lace_score) as avg_lace_score,
        avg(i.los_days) as avg_los_days
    from idx i
    join tier_rates t using (risk_tier)
    group by 1
)

select
    h.hospital_key,
    h.organization_name,
    h.city,
    h.state_code,
    h.ccn,
    h.match_tier,
    h.cms_facility_name,
    h.cms_star_rating,
    h.cms_avg_excess_readmission_ratio,
    h.cms_cohorts_penalized,
    b.index_admissions,
    b.readmissions,
    round(b.readmissions::numeric / b.index_admissions, 4) as observed_rate,
    round(b.expected_readmissions / b.index_admissions, 4) as expected_rate,
    -- O/E is unstable at low volume (1 readmission / 0.02 expected = 50x), so it is only
    -- reported once the hospital meets the minimum-volume threshold.
    case
        when b.index_admissions >= {{ var('scorecard_min_index_admissions') }}
            then round(b.readmissions / nullif(b.expected_readmissions, 0), 3)
    end as oe_ratio,
    round(p.plan_rate, 4) as plan_rate,
    round(b.readmission_cost, 2) as readmission_cost,
    round(b.index_admission_cost, 2) as index_admission_cost,
    round(b.avg_lace_score::numeric, 2) as avg_lace_score,
    round(b.avg_los_days::numeric, 2) as avg_los_days,
    case
        when b.index_admissions < {{ var('scorecard_min_index_admissions') }} then 'insufficient volume'
        when b.readmissions >= 3 and b.readmissions / nullif(b.expected_readmissions, 0) >= 1.2
            then 'review'
        when b.readmissions / nullif(b.expected_readmissions, 0) <= 0.8 then 'preferred'
        else 'monitor'
    end as network_action
from by_hospital b
join {{ ref('dim_hospital') }} h using (hospital_key)
cross join plan p
