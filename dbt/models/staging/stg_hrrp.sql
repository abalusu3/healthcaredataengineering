select
    ccn,
    cohort as hrrp_cohort,
    discharges,
    excess_readmission_ratio,
    predicted_readmission_rate,
    expected_readmission_rate,
    readmissions,
    readmissions_suppressed,
    period_start,
    period_end
from {{ source('raw', 'hrrp') }}
where cohort is not null
