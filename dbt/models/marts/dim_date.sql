-- Calendar spanning the full admission history (Synthea simulates whole lifetimes).
with bounds as (
    select
        date_trunc('year', min(admit_ts))::date as start_date,
        (date_trunc('year', max(discharge_ts)) + interval '2 years' - interval '1 day')::date
            as end_date
    from {{ ref('stg_admissions') }}
)

select
    d::date as date_day,
    extract(year from d)::int as year,
    extract(quarter from d)::int as quarter,
    extract(month from d)::int as month,
    to_char(d, 'Mon') as month_name,
    date_trunc('month', d)::date as month_start,
    extract(isodow from d)::int as iso_day_of_week,
    extract(isodow from d) in (6, 7) as is_weekend
from bounds, generate_series(bounds.start_date, bounds.end_date, interval '1 day') as d
