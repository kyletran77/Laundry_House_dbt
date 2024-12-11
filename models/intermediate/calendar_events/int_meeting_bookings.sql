select * from 

{{ref('stg_meeting_bookings')}}

where meeting_id not in (
    select meeting_id from {{ref('stg_meeting_cancellations')}}
)  and meeting_id not in (
    select meeting_id from {{ref('stg_meeting_reschedules')}}
)