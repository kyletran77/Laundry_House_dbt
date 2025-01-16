-- Goal: Get single segment user profile based on shared anonymous IDs
with segment_identifies as (
    select 
        anonymous_id,
        user_id,
        nullif(trim(email), '') as email,
        nullif(trim(phone), '') as phone_number
    from {{ ref('segment_identifies') }}
    where nullif(trim(email), '') is not null 
       or nullif(trim(phone), '') is not null
),

-- Group by anonymous_id to get unique profiles
unique_profiles as (
    select
        anonymous_id,
        -- Take the latest non-null values
        last_value(user_id ignore nulls) over (
            partition by anonymous_id 
            order by timestamp
            rows between unbounded preceding and unbounded following
        ) as user_id,
        last_value(email ignore nulls) over (
            partition by anonymous_id 
            order by timestamp
            rows between unbounded preceding and unbounded following
        ) as email,
        last_value(phone_number ignore nulls) over (
            partition by anonymous_id 
            order by timestamp
            rows between unbounded preceding and unbounded following
        ) as phone_number
    from segment_identifies
),

-- Final deduplication
final as (
    select distinct
        user_id,
        email,
        phone_number
    from unique_profiles
    where user_id is not null
)

select * from final