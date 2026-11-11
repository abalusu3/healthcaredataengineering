-- LACE index per admission (0-19) and risk tier.
select
    a.admission_id,
    {{ lace_l_points('a.los_days') }} as lace_l,
    case when e.is_emergent then 3 else 0 end as lace_a,
    {{ lace_c_points('c.charlson_score') }} as lace_c,
    least(e.ed_visits_6m, 4) as lace_e,
    c.charlson_score,
    c.comorbidities,
    e.is_emergent,
    e.ed_visits_6m
from {{ ref('stg_admissions') }} a
join {{ ref('int_admission_charlson') }} c using (admission_id)
join {{ ref('int_admission_ed_use') }} e using (admission_id)
