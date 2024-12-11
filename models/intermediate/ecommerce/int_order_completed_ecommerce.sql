select * from  {{ref('stg_order_completed_shopify')}}
where order_id not in (
    select order_id from {{ref('stg_order_cancelled_shopify')}}
)