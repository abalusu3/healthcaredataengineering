select
    member_id,
    encounter_id,
    code as snomed_code,
    description,
    start_date,
    stop_date
from {{ source('raw', 'conditions') }}
