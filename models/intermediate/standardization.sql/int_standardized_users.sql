with user_source as (
    select
        customer_id,
        nullif(trim(email), '') as email,  -- Convert empty strings to nulls
        nullif(trim(phone_number), '') as phone_number,  -- Convert empty strings to nulls
        registration_date,
        last_usage_date,
        total_payment,
        cycle_total,
        balance,
        free_balance
    from {{ ref('stg_customer_sales') }}
),

-- Step 1: Create pairs of matching users based on email
email_matches as (
    select distinct
        least(a.customer_id, b.customer_id) as user_a,
        greatest(a.customer_id, b.customer_id) as user_b,
        a.email as matching_email
    from user_source a
    inner join user_source b
        on a.email = b.email
        and a.customer_id != b.customer_id
        and a.email is not null
),

-- Step 2: Create pairs of matching users based on phone
phone_matches as (
    select distinct
        least(a.customer_id, b.customer_id) as user_a,
        greatest(a.customer_id, b.customer_id) as user_b,
        a.phone_number as matching_phone
    from user_source a
    inner join user_source b
        on a.phone_number = b.phone_number
        and a.customer_id != b.customer_id
        and a.phone_number is not null
),

-- Step 3: Combine all matches
all_matches as (
    select user_a, user_b, 'email' as match_type, matching_email as match_value 
    from email_matches
    union all
    select user_a, user_b, 'phone' as match_type, matching_phone as match_value 
    from phone_matches
),

-- Step 4: Find connected components using a simpler approach
connected_groups as (
    select distinct
        customer_id,
        first_value(customer_id) over (
            partition by group_id
            order by customer_id
        ) as master_user_id
    from (
        select 
            us.customer_id,
            coalesce(
                min(em.group_id),
                min(pm.group_id)
            ) as group_id
        from user_source us
        left join (
            select customer_id, min(email) over (partition by email) as group_id
            from user_source
            where email is not null
        ) em on us.customer_id = em.customer_id
        left join (
            select customer_id, min(phone_number) over (partition by phone_number) as group_id
            from user_source
            where phone_number is not null
        ) pm on us.customer_id = pm.customer_id
        group by us.customer_id
    )
    where group_id is not null
),

-- Step 5: Aggregate user attributes
final as (
    select
        coalesce(cg.master_user_id, us.customer_id) as master_user_id,
        array_agg(distinct us.customer_id) as merged_customer_ids,
        array_agg(distinct us.email ignore nulls) as all_emails,
        array_agg(distinct us.phone_number ignore nulls) as all_phone_numbers,
        min(us.registration_date) as first_registration_date,
        max(us.last_usage_date) as last_usage_date,
        sum(us.total_payment) as total_payments,
        sum(us.cycle_total) as total_cycle_usage,
        sum(us.balance) as combined_balance,
        sum(us.free_balance) as combined_free_balance,
        count(distinct us.customer_id) as number_of_merged_accounts
    from user_source us
    left join connected_groups cg
        on us.customer_id = cg.customer_id
    group by coalesce(cg.master_user_id, us.customer_id)
)

select 
    master_user_id,
    merged_customer_ids,
    all_emails,
    all_phone_numbers,
    first_registration_date,
    last_usage_date,
    total_payments,
    total_cycle_usage,
    combined_balance,
    combined_free_balance,
    number_of_merged_accounts
from final