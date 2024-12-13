{% macro analyze_conversion_pipeline() %}

    {% set analysis_query %}
        -- Source Data Analysis
        with source_leads as (
            select 
                'identifies_source' as step,
                count(*) as total_rows,
                count(distinct user_id) as unique_users,
                cast(min(loaded_at) as timestamp) as earliest_date,
                cast(max(loaded_at) as timestamp) as latest_date
            from {{ source('gtm_server_side', 'identifies') }}
            where user_id is not null
        ),

        source_sales as (
            select 
                'customer_sales_source' as step,
                count(*) as total_rows,
                count(distinct email) as unique_users,
                cast(min(registration_date) as timestamp) as earliest_date,
                cast(max(registration_date) as timestamp) as latest_date
            from {{ ref('stg_customer_sales') }}
            where email is not null
        ),

        -- Staging Layer Analysis
        staged_sessions as (
            select 
                'mart_sessions' as step,
                count(*) as total_rows,
                count(distinct blended_user_id) as unique_users,
                count(distinct session_id) as unique_sessions,
                cast(min(session_start_timestamp) as timestamp) as earliest_date,
                cast(max(session_start_timestamp) as timestamp) as latest_date
            from {{ ref('mart_sessions') }}
        ),

        staged_leads as (
            select 
                'mart_leads' as step,
                count(*) as total_rows,
                count(distinct email) as unique_users,
                cast(min(sign_up_date) as timestamp) as earliest_date,
                cast(max(sign_up_date) as timestamp) as latest_date
            from {{ ref('mart_leads') }}
        ),

        staged_sales as (
            select 
                'mart_sales' as step,
                count(*) as total_rows,
                count(distinct email) as unique_users,
                sum(lifetime_value) as total_revenue,
                cast(min(first_payment_date) as timestamp) as earliest_date,
                cast(max(first_payment_date) as timestamp) as latest_date
            from {{ ref('mart_sales') }}
        ),

        -- Final Model Analysis
        final_model as (
            select 
                'mart_conversions_first_touch' as step,
                conversion_type,
                count(*) as total_rows,
                count(distinct email) as unique_users,
                count(case when first_touch_source is not null then 1 end) as rows_with_attribution,
                sum(revenue) as total_revenue,
                cast(min(conversion_date) as timestamp) as earliest_date,
                cast(max(conversion_date) as timestamp) as latest_date
            from {{ ref('mart_conversions_first_touch') }}
            group by conversion_type
        ),

        -- Attribution Analysis
        attribution_completeness as (
            select 
                'attribution_analysis' as step,
                count(*) as total_conversions,
                count(case when first_touch_source is not null then 1 end) as conversions_with_source,
                count(case when first_touch_medium is not null then 1 end) as conversions_with_medium,
                count(case when first_touch_campaign is not null then 1 end) as conversions_with_campaign,
                avg(case when days_to_convert is not null then days_to_convert end) as avg_days_to_convert,
                cast(null as timestamp) as earliest_date,
                cast(null as timestamp) as latest_date
            from {{ ref('mart_conversions_first_touch') }}
        ),

        -- User Journey Analysis
        user_journey as (
            select 
                'user_journey_analysis' as step,
                count(distinct c.email) as total_users,
                count(distinct case when c.conversion_type = 'lead' then c.email end) as users_with_leads,
                count(distinct case when c.conversion_type = 'sale' then c.email end) as users_with_sales,
                count(distinct case when c.first_touch_source is not null then c.email end) as users_with_attribution,
                cast(null as timestamp) as earliest_date,
                cast(null as timestamp) as latest_date
            from {{ ref('mart_conversions_first_touch') }} c
        )

        -- Output all analyses
        select * from (
            select 'SOURCE DATA' as analysis_type, 
                   step,
                   total_rows,
                   unique_users,
                   earliest_date,
                   latest_date,
                   cast(null as string) as other_metrics
            from source_leads
            union all
            select 'SOURCE DATA',
                   step,
                   total_rows,
                   unique_users,
                   earliest_date,
                   latest_date,
                   cast(null as string)
            from source_sales
            union all
            select 'STAGING LAYER',
                   step,
                   total_rows,
                   unique_users,
                   earliest_date,
                   latest_date,
                   'Sessions: ' || cast(unique_sessions as string)
            from staged_sessions
            union all
            select 'STAGING LAYER',
                   step,
                   total_rows,
                   unique_users,
                   earliest_date,
                   latest_date,
                   cast(null as string)
            from staged_leads
            union all
            select 'STAGING LAYER',
                   step,
                   total_rows,
                   unique_users,
                   earliest_date,
                   latest_date,
                   'Revenue: ' || cast(total_revenue as string)
            from staged_sales
            union all
            select 'FINAL MODEL',
                   step || ' - ' || conversion_type,
                   total_rows,
                   unique_users,
                   earliest_date,
                   latest_date,
                   'Attribution: ' || cast(rows_with_attribution as string) || ' rows, Revenue: ' || cast(total_revenue as string)
            from final_model
            union all
            select 'ATTRIBUTION ANALYSIS',
                   step,
                   total_conversions,
                   conversions_with_source,
                   earliest_date,
                   latest_date,
                   'With Medium: ' || cast(conversions_with_medium as string) || 
                   ', With Campaign: ' || cast(conversions_with_campaign as string) ||
                   ', Avg Days: ' || cast(avg_days_to_convert as string)
            from attribution_completeness
            union all
            select 'USER JOURNEY',
                   step,
                   total_users,
                   users_with_attribution,
                   earliest_date,
                   latest_date,
                   'Leads: ' || cast(users_with_leads as string) || 
                   ', Sales: ' || cast(users_with_sales as string)
            from user_journey
        )
        order by analysis_type, step
    {% endset %}

    {% set results = run_query(analysis_query) %}
    
    {% if execute %}
        {{ log("=== Conversion Pipeline Analysis ===", info=True) }}
        {{ log("", info=True) }}
        {% for row in results.rows %}
            {{ log("=== " ~ row[0] ~ " - " ~ row[1] ~ " ===", info=True) }}
            {{ log("Total Rows: " ~ row[2], info=True) }}
            {{ log("Unique Users: " ~ row[3], info=True) }}
            {% if row[4] is not none %}
                {{ log("Date Range: " ~ row[4] ~ " to " ~ row[5], info=True) }}
            {% endif %}
            {% if row[6] is not none %}
                {{ log("Additional Metrics: " ~ row[6], info=True) }}
            {% endif %}
            {{ log("", info=True) }}
        {% endfor %}
    {% endif %}

{% endmacro %}