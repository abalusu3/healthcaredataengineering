select payer_id, payer_name, payer_type, ownership
from {{ ref('stg_payers') }}
