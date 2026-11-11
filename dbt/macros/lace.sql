{#
  LACE index (van Walraven et al., CMAJ 2010): predicts 30-day death or unplanned readmission.
    L = length of stay, A = acuity (emergent admission), C = Charlson comorbidity index,
    E = ED visits in the prior 6 months.  Total 0-19.
#}

{% macro lace_l_points(los_days) -%}
    case
        when {{ los_days }} < 1 then 0
        when {{ los_days }} < 2 then 1
        when {{ los_days }} < 3 then 2
        when {{ los_days }} < 4 then 3
        when {{ los_days }} < 7 then 4
        when {{ los_days }} < 14 then 5
        else 7
    end
{%- endmacro %}

{% macro lace_c_points(charlson_score) -%}
    case when {{ charlson_score }} >= 4 then 5 else {{ charlson_score }} end
{%- endmacro %}

{% macro lace_risk_tier(lace_score) -%}
    case
        when {{ lace_score }} >= {{ var('lace_high_risk_min') }} then 'high'
        when {{ lace_score }} >= {{ var('lace_moderate_risk_min') }} then 'moderate'
        else 'low'
    end
{%- endmacro %}
