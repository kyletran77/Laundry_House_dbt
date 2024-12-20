

{% macro validate_multi_touch_attribution() %}

    -- Check revenue totals
    {% set revenue_query %}
        select 
            'Revenue Check' as metric,
            (select sum(lifetime_value) from {{ ref('mart_sales') }} 
             where cast(first_payment_date as timestamp) > timestamp('2024-12-04')) as source_total,
            (select sum(weighted_revenue) from {{ ref('mart_conversions_multi_touch') }}
             where conversion_date > timestamp('2024-12-04')) as attributed_total
    {% endset %}

    -- Check lead totals
    {% set leads_query %}
        select 
            'Leads Check' as metric,
            (select count(*) from {{ ref('mart_leads') }}
             where cast(sign_up_date as timestamp) > timestamp('2024-12-04')) as source_total,
            (select sum(weighted_lead) from {{ ref('mart_conversions_multi_touch') }}
             where conversion_date > timestamp('2024-12-04')) as attributed_total
    {% endset %}

    -- Check sales totals
    {% set sales_query %}
        select 
            'Sales Check' as metric,
            (select count(*) from {{ ref('mart_sales') }}
             where cast(first_payment_date as timestamp) > timestamp('2024-12-04')) as source_total,
            (select sum(weighted_sale) from {{ ref('mart_conversions_multi_touch') }}
             where conversion_date > timestamp('2024-12-04')) as attributed_total
    {% endset %}

    {% set revenue_results = run_query(revenue_query) %}
    {% set leads_results = run_query(leads_query) %}
    {% set sales_results = run_query(sales_query) %}

    {% if execute %}
        {{ log("=== Multi-Touch Attribution Validation ===", info=True) }}
        {{ log("", info=True) }}
        {{ log("Revenue:", info=True) }}
        {{ log("  Source: " ~ revenue_results.columns[1].values()[0], info=True) }}
        {{ log("  Attributed: " ~ revenue_results.columns[2].values()[0], info=True) }}
        {{ log("", info=True) }}
        {{ log("Leads:", info=True) }}
        {{ log("  Source: " ~ leads_results.columns[1].values()[0], info=True) }}
        {{ log("  Attributed: " ~ leads_results.columns[2].values()[0], info=True) }}
        {{ log("", info=True) }}
        {{ log("Sales:", info=True) }}
        {{ log("  Source: " ~ sales_results.columns[1].values()[0], info=True) }}
        {{ log("  Attributed: " ~ sales_results.columns[2].values()[0], info=True) }}
    {% endif %}

{% endmacro %}