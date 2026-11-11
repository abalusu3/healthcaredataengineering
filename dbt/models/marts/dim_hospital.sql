-- One row per care organization; enriched with CMS attributes when entity resolution
-- matched it to a CCN (current version from the SCD2 snapshot).
with cms_current as (
    select * from {{ ref('cms_hospitals_snapshot') }} where dbt_valid_to is null
),

hrrp as (
    select
        ccn,
        round(avg(excess_readmission_ratio)::numeric, 4) as cms_avg_excess_readmission_ratio,
        count(*) filter (where excess_readmission_ratio > 1) as cms_cohorts_penalized,
        count(*) filter (where excess_readmission_ratio is not null) as cms_cohorts_reported
    from {{ ref('stg_hrrp') }}
    group by 1
)

select
    {{ dbt_utils.generate_surrogate_key(['o.organization_id']) }} as hospital_key,
    o.organization_id,
    o.organization_name,
    o.city,
    o.state_code,
    o.zip5,
    o.lat,
    o.lon,
    x.ccn,
    coalesce(x.match_tier, 'unmatched') as match_tier,
    x.match_method,
    c.facility_name as cms_facility_name,
    c.hospital_type,
    c.ownership,
    c.emergency_services,
    c.overall_rating as cms_star_rating,
    h.cms_avg_excess_readmission_ratio,
    h.cms_cohorts_penalized,
    h.cms_cohorts_reported
from {{ ref('stg_organizations') }} o
left join {{ ref('stg_hospital_xref') }} x using (organization_id)
left join cms_current c on c.ccn = x.ccn
left join hrrp h on h.ccn = x.ccn
