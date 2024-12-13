{% macro check_sales_sessions() %}
    {% set check_query %}
        with sales_sessions as (
            select
                c.email,
                c.conversion_type,
                c.conversion_date,
                c.revenue,
                min(s.session_start_timestamp) as first_session_date,
                count(distinct s.session_id) as total_sessions
            from {{ ref('mart_conversions_first_touch') }} c
            left join {{ ref('mart_sessions') }} s
                on c.email = s.blended_user_id
                and s.session_start_timestamp <= c.conversion_date
            where c.conversion_type = 'sale'
            group by 1, 2, 3, 4
        )

        select 
            count(*) as total_sales,
            count(case when first_session_date is not null then 1 end) as sales_with_sessions,
            round(avg(total_sessions), 2) as avg_sessions_per_sale,
            count(case when first_session_date > conversion_date then 1 end) as sessions_after_sale
        from sales_sessions
    {% endset %}

    {% set results = run_query(check_query) %}
    
    {% if execute %}
        {{ log("=== Sales Session Analysis ===", info=True) }}
        {{ log("", info=True) }}
        {% for row in results.rows %}
            {{ log("Total Sales: " ~ row[0], info=True) }}
            {{ log("Sales with Sessions: " ~ row[1], info=True) }}
            {{ log("Average Sessions per Sale: " ~ row[2], info=True) }}
            {{ log("Sales with Sessions After Sale Date: " ~ row[3], info=True) }}
        {% endfor %}
    {% endif %}

{% endmacro %}