-- this model defines conversion events
select
    user_id,
    event_timestamp,
    "form_submitted" as conversion_event,

    "lead" as conversion_type,
    form_name as conversion_detail,
    0 as conversion_amount,
    0 as net_conversion_amount,

from {{ ref("int_form_submitted") }}

union all


select
    user_id,
    event_timestamp,
    "checkout_started" as conversion_event,

    "ecommerce" as conversion_type,
    "checkout_started" as conversion_detail,
    0 as conversion_amount,
    0 as net_conversion_amount,

from {{ ref("int_checkout_started") }}

union all

select
    user_id,
    event_timestamp,
    "order_completed" as conversion_event,
    "ecommerce" as conversion_type,
    "order_completed" as conversion_detail,

    total as conversion_amount,
    net_total as net_conversion_amount
from {{ ref("int_order_completed_ecommerce") }}

union all

select
    user_id,
    event_timestamp,
    "invoice_paid" as conversion_event,
    "highticket" as conversion_type,
    "invoice_paid" as conversion_detail,
    total as conversion_amount,
    net_total as net_conversion_amount
from {{ ref("stg_sales_invoice_paid") }}


union all
select
    user_id,
    event_timestamp,
    "estimate_mark_accepted" as conversion_event,
    "highticket" as conversion_type,
    reference as conversion_detail,
    total as conversion_amount,
    net_total as net_conversion_amount
from {{ ref("stg_estimate_accepted_by_contact") }}

union all
select
    user_id,
    event_timestamp,
    "meeting_booked" as conversion_event,
    "highticket" as conversion_type,
    meeting_name as conversion_detail,
    0 as conversion_amount,
    0 as net_conversion_amount
from {{ ref("int_meeting_bookings") }}


