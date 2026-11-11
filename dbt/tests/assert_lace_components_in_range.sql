-- LACE components must stay within their published point ranges.
select admission_id
from {{ ref('fct_admissions') }}
where lace_l not between 0 and 7
   or lace_a not in (0, 3)
   or lace_c not between 0 and 5
   or lace_e not between 0 and 4
