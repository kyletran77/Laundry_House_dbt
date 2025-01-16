-- Step 1: Create a Single Unified Contact Table with standardized fields
with unified_contacts as (
    select
        'customer_sales' as source_table,
        nullif(trim(email), '') as email_clean,
        nullif(trim(phone_number), '') as phone_clean
    from {{ ref('stg_customer_sales') }}
    where nullif(trim(email), '') is not null 
       or nullif(trim(phone_number), '') is not null
    union all
    select
        'segment' as source_table,
        nullif(trim(email), '') as email_clean,
        nullif(trim(phone_number), '') as phone_clean
    from {{ref('stage_segment_users')}}

),

-- Step 2: Assign Initial Temporary IDs
initial_ids as (
    select
        *,
        row_number() over (order by contact_id) as temp_id
    from unified_contacts
),

-- Step 3: First Iteration - Find direct matches
iteration_1 as (
    select
        a.email_clean,
        a.phone_clean,
        min(b.temp_id) as temp_id,
    from initial_ids a
    join initial_ids b
        on (
            a.email_clean = b.email_clean and a.email_clean is not null
        ) or (
            a.phone_clean = b.phone_clean and a.phone_clean is not null
        )
    group by 
        a.email_clean,
        a.phone_clean,
),

-- Step 4: Second Iteration - Catch multi-hop connections
iteration_2 as (
    select
        a.email_clean,
        a.phone_clean,
        min(b.temp_id) as temp_id,
    from iteration_1 a
    join iteration_1 b
        on (
            a.email_clean = b.email_clean and a.email_clean is not null
        ) or (
            a.phone_clean = b.phone_clean and a.phone_clean is not null
        )
    group by 
        a.email_clean,
        a.phone_clean
),

-- Step 5: Third Iteration - Final pass to ensure full convergence
iteration_3 as (
    select
        a.email_clean,
        a.phone_clean,
        min(b.temp_id) as temp_id,
    from iteration_2 a
    join iteration_2 b
        on (
            a.email_clean = b.email_clean and a.email_clean is not null
        ) or (
            a.phone_clean = b.phone_clean and a.phone_clean is not null
        )
    group by 
        a.email_clean,
        a.phone_clean
),




-- Step 6: Final aggregation of user attributes
final as (
    select
        temp_id as user_id,
        array_agg(distinct contact_id) as contact_ids,
        array_agg(distinct email_clean ignore nulls) as emails,
        array_agg(distinct phone_clean ignore nulls) as phone_numbers,
        min(registration_date) as first_registration_date,
        max(last_usage_date) as last_usage_date,
        sum(total_payment) as total_payments,
        sum(cycle_total) as total_cycle_usage,
        sum(balance) as combined_balance,
        sum(free_balance) as combined_free_balance,
        count(distinct contact_id) as number_of_merged_accounts
    from iteration_3
    group by temp_id
)

select * from final