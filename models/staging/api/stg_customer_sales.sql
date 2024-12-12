with source as (
    select * from {{ source('api', 'accounts') }}
),

renamed as (
    select
        -- ids
        cast(card_number as string) as customer_id,
        
        -- customer info
        lower(trim(email_address)) as email,
        lower(trim(mobile_number)) as phone_number,
        lower(trim(first_name)) as first_name,
        lower(trim(last_name)) as last_name,
        
        -- payment info
        cast(payment_total as numeric) as total_payment,
        cast(cycle_total as numeric) as cycle_total,
        cast(balance as numeric) as balance,
        cast(free_balance as numeric) as free_balance,
        
        -- dates
        registration_date,  -- already in datetime format
        last_usage as last_usage_date,  -- already in datetime format
        
        -- flags
        card_type,
        cast(is_banned as boolean) as is_banned,
        cast(is_managed as boolean) as is_managed,
        
        -- opt-in preferences
        cast(opt_in_cycle_done_sms as boolean) as opt_in_cycle_done_sms,
        cast(opt_in_receipts_email as boolean) as opt_in_receipts_email,
        cast(opt_in_promos_sms as boolean) as opt_in_promos_sms,
        cast(opt_in_promos_email as boolean) as opt_in_promos_email

    from source
),

final as (
    select
        *,
        registration_date as payment_date  -- aliasing registration_date as payment_date
    from renamed
    where customer_id is not null  -- ensuring we only get valid customer records
)

select * from final