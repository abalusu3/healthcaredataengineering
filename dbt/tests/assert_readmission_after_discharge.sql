-- The readmission must start after the index discharge and be a different episode.
select admission_id
from {{ ref('fct_admissions') }}
where readmit_30d
  and (readmit_admit_ts < discharge_ts or readmit_admission_id = admission_id)
