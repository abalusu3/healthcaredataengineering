select
    organization_id,
    name as organization_name,
    city,
    state_code,
    zip5,
    lat,
    lon
from {{ source('raw', 'organizations') }}
