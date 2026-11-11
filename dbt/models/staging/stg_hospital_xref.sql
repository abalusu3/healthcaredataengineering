select
    organization_id,
    ccn,
    cms_name,
    match_tier,
    match_method,
    match_score
from {{ source('raw', 'hospital_xref') }}
