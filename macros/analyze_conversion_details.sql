{% macro analyze_conversion_details() %}
    {% set analysis_query %}
        -- Check source data counts
        with source_counts as (
            select 
                'leads' as type,
                count(*) as total,
                count(distinct email) as unique_emails
            from {{ ref('mart_leads') }}
            where cast(sign_up_date as datetime) >= datetime('2023-12-04')
            
            union all
            
            select 
                'sales' as type,
                count(*) as total,
                count(distinct email) as unique_emails
            from {{ ref('mart_sales') }}
            where cast(first_payment_date as datetime) >= datetime('2023-12-04')
        ),
        
        -- Check final data counts
        final_counts as (
            select
                conversion_type,
                count(*) as total,
                count(distinct email) as unique_emails,
                count(case when first_touch_source is not null then 1 end) as with_attribution
            from {{ ref('mart_conversions_first_touch') }}
            group by conversion_type
        )
        
        select 
            'Source Data' as check_type,
            type as conversion_type,
            total,
            unique_emails
        from source_counts
        
        union all
        
        select 
            'Final Model' as check_type,
            conversion_type,
            total,
            unique_emails
        from final_counts
        order by check_type, conversion_type
    {% endset %}

    {% set results = run_query(analysis_query) %}
    
    {% if execute %}
        {{ log("=== Conversion Analysis ===", info=True) }}
        {{ log("Check Type | Conv Type | Total | Unique Emails", info=True) }}
        {{ log("-" * 50, info=True) }}
        {% for row in results.rows %}
            {{ log(row[0] ~ " | " ~ row[1] ~ " | " ~ row[2] ~ " | " ~ row[3], info=True) }}
        {% endfor %}
    {% endif %}
{% endmacro %}