Select everything from standardized user
join with everything from stage customer sales
groupby user id
-- Step 6: Final aggregation of user attributes
final as (
    select
        user_id,
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