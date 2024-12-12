{{ config(
    materialized = 'incremental',
    unique_key = 'conversion_id',
    sort = 'conversion_date',
    partition_by = {'field': 'conversion_date', 'data_type': 'timestamp'},
    cluster_by = ['email', 'conversion_type']
) }}

with first_touch_sessions as (
    select
        blended_user_id,
        anonymous_id,
        session_id,
        session_start_timestamp,
        -- marketing attribution
        cast(utm_source as string) as utm_source,
        cast(utm_medium as string) as utm_medium,
        cast(referrer_source as string) as referrer_source,
        cast(referrer_medium as string) as referrer_medium,
        cast(channel_source as string) as channel_source,
        cast(channel_medium as string) as channel_medium,
        cast(utm_campaign as string) as utm_campaign,
        cast(utm_term as string) as utm_term,
        cast(utm_content as string) as utm_content,
        cast(page_referrer as string) as page_referrer,
        cast(page_referrer_host as string) as page_referrer_host
    from {{ ref('mart_sessions') }}
    qualify row_number() over (
        partition by blended_user_id 
        order by session_start_timestamp asc
    ) = 1
),

conversions as (
    -- Lead conversions
    select
        'lead' as conversion_type,
        email,
        cast(sign_up_date as timestamp) as conversion_date,  -- Cast to timestamp
        cast(0 as numeric) as revenue,
        concat('lead_', email) as conversion_id
    from {{ ref('mart_leads') }}
    
    union all
    
    -- Sales conversions
    select
        'sale' as conversion_type,
        email,
        cast(first_payment_date as timestamp) as conversion_date,  -- Cast to timestamp
        lifetime_value as revenue,
        concat('sale_', email) as conversion_id
    from {{ ref('mart_sales') }}
),

final as (
    select
        c.conversion_id,
        c.conversion_type,
        c.email,
        c.conversion_date,
        c.revenue,
        -- First touch attribution
        coalesce(fs.utm_source, fs.referrer_source, fs.channel_source, '(direct)') as first_touch_source,
        coalesce(fs.utm_medium, fs.referrer_medium, fs.channel_medium, '(none)') as first_touch_medium,
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
)

select * from final