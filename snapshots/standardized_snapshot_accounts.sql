{% snapshot standardized_accounts_snapshot %}

{{ config(
    target_schema='snapshots',      -- Where snapshots will be stored
    unique_key='user_id',          -- Primary identifier for each record
    strategy='timestamp',          -- How DBT detects changes
    updated_at='_fivetran_synced', -- Field that indicates when record was updated
    invalidate_hard_deletes=True   -- Handle deleted records
) }}

select 
    cast(user_id as string) as user_id,
    contact_ids,
    emails,
    phone_numbers,
    first_registration_date,
    last_usage_date,
    total_payments,
    total_cycle_usage,
    combined_balance,
    combined_free_balance,
    number_of_merged_accounts,
    _fivetran_synced
from {{ ref('int_standardized_users') }}

{% endsnapshot %}