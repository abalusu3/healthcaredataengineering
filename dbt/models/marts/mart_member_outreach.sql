-- Care-management work list: members discharged in the last 30 days of data who have not
-- been readmitted yet, ranked by LACE risk. This is the population a transitional-care
-- program would call within 48h of discharge.
with data_end as (
    select max(discharge_ts) as data_end_ts from {{ ref('fct_admissions') }}
),

latest as (
    select
        f.*,
        row_number() over (partition by f.member_key order by f.discharge_ts desc) as rn
    from {{ ref('fct_admissions') }} f
)

select
    row_number() over (order by l.lace_score desc, l.discharge_ts desc) as priority_rank,
    l.member_key,
    l.admission_id,
    h.organization_name as discharging_hospital,
    l.discharge_date,
    (d.data_end_ts::date - l.discharge_date) as days_since_discharge,
    l.payer_type,
    l.hrrp_cohort,
    l.lace_score,
    l.risk_tier,
    l.charlson_score,
    l.comorbidities,
    l.ed_visits_6m,
    l.los_days
from latest l
cross join data_end d
join {{ ref('dim_hospital') }} h using (hospital_key)
where l.rn = 1
  and not l.died_in_hospital
  and not l.is_planned
  and l.discharge_ts >= d.data_end_ts - interval '{{ var("readmit_window_days") }} days'
  and l.risk_tier in ('high', 'moderate')
