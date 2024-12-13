-- Create a new file in macros/validate_conversions.sql
{% macro validate_conversions() %}

    {% set validation_query %}
        select count(*) as conversion_count
        from {{ ref('mart_conversions_first_touch') }}
    {% endset %}

    {% set results = run_query(validation_query) %}
    
    {% if execute %}
        {% set count = results.columns[0].values()[0] %}
        {{ log("Total conversions: " ~ count, info=True) }}
    {% endif %}

{% endmacro %}