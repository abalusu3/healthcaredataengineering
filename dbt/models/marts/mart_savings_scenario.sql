-- Baseline for the "what if we fund transitional care" scenario, by LACE risk tier.
-- Program assumptions are dbt vars (overridable per run); the API recomputes for a slider.
with base as (
    select
        risk_tier,
        count(*) as index_admissions,
        count(distinct member_key) as members,
        sum(readmit_30d::int) as readmissions,
        sum(readmit_cost) as readmission_cost
    from {{ ref('fct_admissions') }}
    where is_index_eligible
    group by 1
)

select
    risk_tier,
    index_admissions,
    members,
    readmissions,
    round(readmissions::numeric / nullif(index_admissions, 0), 4) as readmit_rate,
    round(readmission_cost, 2) as readmission_cost,
    round(readmission_cost / nullif(readmissions, 0), 2) as avg_cost_per_readmission,
    {{ var('outreach_cost_per_member') }} as assumed_outreach_cost_per_discharge,
    {{ var('outreach_effectiveness_pct') }} as assumed_readmission_reduction_pct,
    round(index_admissions * {{ var('outreach_cost_per_member') }}::numeric, 2) as program_cost,
    round(readmission_cost * {{ var('outreach_effectiveness_pct') }} / 100.0, 2) as gross_savings,
    round(
        readmission_cost * {{ var('outreach_effectiveness_pct') }} / 100.0
        - index_admissions * {{ var('outreach_cost_per_member') }}, 2
    ) as net_savings
from base
