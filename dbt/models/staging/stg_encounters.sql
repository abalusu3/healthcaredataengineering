select
    encounter_id,
    member_id,
    organization_id,
    provider_id,
    payer_id,
    start_ts,
    stop_ts,
    encounter_class,
    encounter_code,
    encounter_description,
    reason_code,
    total_claim_cost,
    payer_coverage,
    source_state
from {{ source('raw', 'encounters') }}
