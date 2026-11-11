-- No index admission may point at a planned admission as its readmission.
select f.admission_id
from {{ ref('fct_admissions') }} f
join {{ ref('fct_admissions') }} r on r.admission_id = f.readmit_admission_id
where f.readmit_30d and r.is_planned
