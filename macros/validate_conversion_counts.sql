{% macro validate_conversion_counts() %}

    {% set source_query %}
        with lead_counts as (
            select count(*) as lead_count
            from {{ ref('mart_leads') }}
            where cast(sign_up_date as datetime) >= datetime('2023-12-04')
        ),
        sale_counts as (
            select count(*) as sale_count
            from {{ ref('mart_sales') }}
            where cast(first_payment_date as datetime) >= datetime('2023-12-04')
        ),
        final_counts as (
            select count(*) as final_count
            from {{ ref('mart_conversions_first_touch') }}
        )
        select 
            l.lead_count,
            s.sale_count,
            (l.lead_count + s.sale_count) as total_source_conversions,
            f.final_count as total_final_conversions,
            case 
                when (l.lead_count + s.sale_count) = f.final_count then 'MATCH ✓'
                else 'MISMATCH ✗'
            end as validation_result
        from lead_counts l, sale_counts s, final_counts f
    {% endset %}

    {% set results = run_query(source_query) %}
    
    {% if execute %}
        {% set leads = results.columns[0].values()[0] %}
        {% set sales = results.columns[1].values()[0] %}
        {% set source_total = results.columns[2].values()[0] %}
        {% set final_total = results.columns[3].values()[0] %}
        {% set result = results.columns[4].values()[0] %}
        {{ log("=== Conversion Count Validation ===", info=True) }}
        {{ log("Leads: " ~ leads, info=True) }}
        {{ log("Sales: " ~ sales, info=True) }}
        {{ log("Total Source Conversions: " ~ source_total, info=True) }}
        {{ log("Total Final Conversions: " ~ final_total, info=True) }}
        {{ log("Validation Result: " ~ result, info=True) }}
    {% endif %}

{% endmacro %}