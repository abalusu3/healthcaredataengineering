select
    ccn,
    facility_name,
    city,
    state_code,
    zip5,
    county,
    hospital_type,
    ownership,
    emergency_services,
    overall_rating,
    readm_measures_better,
    readm_measures_worse
from {{ source('raw', 'cms_hospitals') }}
