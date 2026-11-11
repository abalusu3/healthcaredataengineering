-- Charlson Comorbidity Index per admission, from conditions active on the admit date.
-- Hierarchy rules: complicated diabetes supersedes uncomplicated; metastatic tumor
-- supersedes malignancy (each category counted once at its highest weight).
with admissions as (
    select admission_id, member_id, admit_ts::date as admit_date
    from {{ ref('stg_admissions') }}
),

active_conditions as (
    select a.admission_id, m.charlson_category, max(m.weight) as weight
    from admissions a
    join {{ ref('stg_conditions') }} c
        on c.member_id = a.member_id
       and c.start_date <= a.admit_date
       and (c.stop_date is null or c.stop_date >= a.admit_date)
    join {{ ref('charlson_condition_map') }} m
        on m.snomed_code = c.snomed_code
    group by 1, 2
),

flags as (
    select
        admission_id,
        bool_or(charlson_category = 'diabetes_complicated') as has_dm_complicated,
        bool_or(charlson_category = 'metastatic_solid_tumor') as has_metastatic
    from active_conditions
    group by 1
)

select
    a.admission_id,
    coalesce(sum(ac.weight) filter (
        where not (ac.charlson_category = 'diabetes_uncomplicated' and f.has_dm_complicated)
          and not (ac.charlson_category = 'malignancy' and f.has_metastatic)
    ), 0)::int as charlson_score,
    count(ac.charlson_category)::int as charlson_categories,
    coalesce(array_to_string(array_agg(ac.charlson_category order by ac.charlson_category)
        filter (where ac.charlson_category is not null), ', '), '') as comorbidities
from admissions a
left join active_conditions ac on ac.admission_id = a.admission_id
left join flags f on f.admission_id = a.admission_id
group by a.admission_id
