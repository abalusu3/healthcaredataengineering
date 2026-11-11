-- ED utilisation before each admission: drives LACE "A" (acuity) and "E" (ED visits).
--   acuity: an emergency/urgent-care visit in the 24h before admission = emergent admission
--   ed_visits_6m: emergency visits in the 6 months before admission (excluding that 24h one)
with admissions as (
    select admission_id, member_id, admit_ts
    from {{ ref('stg_admissions') }}
),

ed as (  -- small pre-filtered set so the range join stays cheap
    select member_id, start_ts, encounter_class
    from {{ ref('stg_encounters') }}
    where encounter_class in ('emergency', 'urgentcare')
)

select
    a.admission_id,
    coalesce(bool_or(ed.start_ts >= a.admit_ts - interval '24 hours'), false) as is_emergent,
    count(*) filter (
        where ed.encounter_class = 'emergency'
          and ed.start_ts < a.admit_ts - interval '24 hours'
    )::int as ed_visits_6m
from admissions a
left join ed
    on ed.member_id = a.member_id
   and ed.start_ts >= a.admit_ts - interval '6 months'
   and ed.start_ts <= a.admit_ts
group by a.admission_id
