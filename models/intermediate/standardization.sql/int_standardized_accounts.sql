-- Step 1: Select everything from standardized users
with standardized_users as (
    select * from {{ ref('int_standardized_users') }}
),

-- Step 2: Join with everything from stage customer sales
accounts_with_users as (
    select 
        su.user_id,
        cs.*
    from standardized_users su
    join {{ ref('stg_customer_sales') }} cs
        on cs.email = any(su.emails)
        or cs.phone_number = any(su.phone_numbers)
),

-- Step 3: Group by user_id
final as (
    select
        user_id,
        min(registration_date) as first_registration_date,
        max(last_usage_date) as last_usage_date,
        sum(total_payment) as total_payments,
        sum(cycle_total) as total_cycle_usage,
        sum(balance) as combined_balance,
        sum(free_balance) as combined_free_balance,
        count(distinct contact_id) as number_of_merged_accounts
    from accounts_with_users
    group by user_id
)

select * from final