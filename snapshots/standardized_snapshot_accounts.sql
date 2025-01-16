{% snapshot standardized_accounts_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='user_id',
        strategy='check',
        check_cols=[
            'total_payments',
            'total_cycle_usage',
            'combined_balance',
            'combined_free_balance',
            'last_usage_date',
            'number_of_merged_accounts'
        ],
        invalidate_hard_deletes=True
    )
}}

with combined_data as (
    select 
        a.*,
        u.emails,
        u.phone_numbers
    from {{ ref('int_standardized_accounts') }} a
    join {{ ref('int_standardized_users') }} u using (user_id)
)

select 
    cast(user_id as string) as user_id,
    emails,
    phone_numbers,
    first_registration_date,
    last_usage_date,
    total_payments,
    total_cycle_usage,
    combined_balance,
    combined_free_balance,
    number_of_merged_accounts,
    array_to_string(emails, ',') as emails_string,
    array_to_string(phone_numbers, ',') as phone_numbers_string
from combined_data

{% endsnapshot %}