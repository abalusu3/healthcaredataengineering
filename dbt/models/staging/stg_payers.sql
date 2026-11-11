select
    payer_id,
    name as payer_name,
    ownership,
    case name
        when 'Medicare' then 'Medicare'
        when 'Medicaid' then 'Medicaid'
        when 'Dual Eligible' then 'Dual'
        when 'NO_INSURANCE' then 'Uninsured'
        else 'Commercial'
    end as payer_type
from {{ source('raw', 'payers') }}
