{{ config(
    materialized = 'incremental',
    unique_key = 'activity_id',
    sort = 'activity_date',
    partition_by = {'field': 'activity_date', 'data_type': 'timestamp'},
    cluster_by = ['user_id']
) }}

with snapshot_data as (
    select
        user_id,
        total_payments,
        total_cycle_usage,
        combined_balance,
        last_usage_date,
        first_registration_date,
        dbt_valid_from as activity_date,
        lead(dbt_valid_from) over (
            partition by user_id 
            order by dbt_valid_from
        ) as next_activity_date,
        lag(total_payments) over (
            partition by user_id 
            order by dbt_valid_from
        ) as previous_total_payments,
        lag(total_cycle_usage) over (
            partition by user_id 
            order by dbt_valid_from
        ) as previous_cycle_usage,
        lag(last_usage_date) over (
            partition by user_id 
            order by dbt_valid_from
        ) as previous_usage_date
    from {{ ref('standardized_accounts_snapshot') }}
),

daily_metrics as (
    select
        user_id,
        activity_date,
        -- Revenue metrics
        total_payments - coalesce(previous_total_payments, 0) as daily_revenue,
        total_payments as lifetime_value,
        
        -- Usage metrics
        total_cycle_usage - coalesce(previous_cycle_usage, 0) as daily_cycles,
        total_cycle_usage as total_cycles,
        
        -- Time-based metrics
        date_diff(
            cast(activity_date as date),
            cast(first_registration_date as date),
            day
        ) as days_since_registration,
        
        date_diff(
            cast(activity_date as date),
            cast(coalesce(previous_usage_date, first_registration_date) as date),
            day
        ) as days_since_last_activity,

        -- Status flags
        case
            when date_diff(
                cast(activity_date as date),
                cast(coalesce(previous_usage_date, first_registration_date) as date),
                day
            ) > 30 then true
            else false
        end as is_churned,

        case
            when previous_total_payments is null then true
            else false
        end as is_new_user,

        -- Usage patterns
        case
            when total_cycle_usage - coalesce(previous_cycle_usage, 0) > 0 then
                case
                    when date_diff(
                        cast(activity_date as date),
                        cast(coalesce(previous_usage_date, first_registration_date) as date),
                        day
                    ) <= 7 then 'frequent'
                    when date_diff(
                        cast(activity_date as date),
                        cast(coalesce(previous_usage_date, first_registration_date) as date),
                        day
                    ) <= 30 then 'regular'
                    else 'occasional'
                end
            else 'inactive'
        end as user_pattern,

        combined_balance as current_balance

    from snapshot_data
),

final as (
    select
        concat(user_id, '_', cast(activity_date as string)) as activity_id,
        user_id,
        activity_date,
        
        -- Revenue metrics
        daily_revenue,
        lifetime_value,
        
        -- Usage metrics
        daily_cycles,
        total_cycles,
        
        -- Time metrics
        days_since_registration,
        days_since_last_activity,
        
        -- Status
        is_churned,
        is_new_user,
        user_pattern,
        
        -- Balance
        current_balance,
        
        -- Rolling metrics (30-day window)
        sum(daily_revenue) over (
            partition by user_id
            order by unix_seconds(activity_date)
            range between 2592000 preceding and current row  -- 30 days in seconds
        ) as rolling_30d_revenue,
        
        sum(daily_cycles) over (
            partition by user_id
            order by unix_seconds(activity_date)
            range between 2592000 preceding and current row  -- 30 days in seconds
        ) as rolling_30d_cycles
        
    from daily_metrics
)

select * from final
{% if is_incremental() %}
    where activity_date > (select max(activity_date) from {{ this }})
{% endif %}