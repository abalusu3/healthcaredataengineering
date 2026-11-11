-- Monthly readmission KPIs by line of business, state and HRRP cohort.
select
    date_trunc('month', discharge_date)::date as discharge_month,
    payer_type,
    source_state,
    hrrp_cohort,
    count(*) as index_admissions,
    sum(readmit_30d::int) as readmissions,
    round(avg(readmit_30d::int)::numeric, 4) as readmit_rate,
    round(sum(readmit_cost), 2) as readmission_cost,
    round(avg(lace_score)::numeric, 2) as avg_lace_score
from {{ ref('fct_admissions') }}
where is_index_eligible
group by 1, 2, 3, 4
