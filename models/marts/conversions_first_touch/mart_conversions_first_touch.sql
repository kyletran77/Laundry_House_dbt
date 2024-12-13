{{ config(
    materialized = 'incremental',
    unique_key = 'conversion_id',
    sort = 'conversion_date',
    partition_by = {'field': 'conversion_date', 'data_type': 'timestamp'},
    cluster_by = ['email', 'conversion_type']
) }}

with conversions as (
    -- Lead conversions
    select
        'lead' as conversion_type,
        email,
        cast(sign_up_date as timestamp) as conversion_date,
        cast(0 as numeric) as revenue,
        concat('lead_', email) as conversion_id
    from {{ ref('mart_leads') }}
    where cast(sign_up_date as timestamp) > timestamp('2024-12-04')  -- After Dec 4th
    
    union all
    
    -- Sales conversions
    select
        'sale' as conversion_type,
        email,
        cast(first_payment_date as timestamp) as conversion_date,
        lifetime_value as revenue,
        concat('sale_', email) as conversion_id
    from {{ ref('mart_sales') }}
    where cast(first_payment_date as timestamp) > timestamp('2024-12-04')  -- After Dec 4th
),

first_touch_sessions as (
    select
        blended_user_id,
        anonymous_id,
        session_id,
        session_start_timestamp,
        coalesce(
            cast(utm_source as string),
            cast(referrer_source as string),
            cast(channel_source as string),
            '(direct)'
        ) as source,
        coalesce(
            cast(utm_medium as string),
            cast(referrer_medium as string),
            cast(channel_medium as string),
            '(none)'
        ) as medium,
        utm_campaign,
        utm_term,
        utm_content,
        page_referrer,
        page_referrer_host
    from {{ ref('mart_sessions') }}
    where session_start_timestamp > timestamp('2024-12-04')  -- After Dec 4th
),

final as (
    select distinct
        c.conversion_id,
        c.conversion_type,
        c.email,
        c.conversion_date,
        c.revenue,
        -- First touch attribution
        fs.source as first_touch_source,
        fs.medium as first_touch_medium,
        fs.utm_campaign as first_touch_campaign,
        fs.utm_term as first_touch_term,
        fs.utm_content as first_touch_content,
        fs.page_referrer as first_touch_referrer,
        fs.page_referrer_host as first_touch_referrer_host,
        fs.session_start_timestamp as first_touch_timestamp,
        fs.session_id as first_touch_session_id,
        -- Time to convert
        timestamp_diff(
            c.conversion_date,
            fs.session_start_timestamp,
            day
        ) as days_to_convert
    from conversions c
    left join first_touch_sessions fs 
        on c.email = fs.blended_user_id
        and fs.session_start_timestamp <= c.conversion_date  -- Only include sessions before conversion
    qualify row_number() over (
        partition by c.conversion_id 
        order by fs.session_start_timestamp asc
    ) = 1  -- Get the first session for each conversion
)

select * from final