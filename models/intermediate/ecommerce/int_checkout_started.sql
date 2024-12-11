with blended_user_id as (
select stg_checkout.* except (user_id), stg_users.* except(anonymous_id)  from  {{ref('stg_checkout_started')}} stg_checkout

left join {{ref("stg_users_map")}} stg_users

using(anonymous_id)

)

select * except(user_id, anonymous_id), coalesce(user_id, anonymous_id) as user_id from blended_user_id



