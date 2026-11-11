-- A readmission can only be counted against an index-eligible admission.
select admission_id
from {{ ref('fct_admissions') }}
where readmit_30d and not is_index_eligible
