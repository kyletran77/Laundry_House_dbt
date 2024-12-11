select
  sessionized_events.timestamp,
  sessionized_events.blended_user_id,
  sessionized_events.track_id,
  sessionized_events.session_index,
  sessionized_events.session_id,
  track_events.event_name
from 
  {{ref('int_sessionized_events')}} as sessionized_events
  left join {{ref('stg_tracks_sgtm')}} as track_events
    on sessionized_events.track_id = track_events.track_id
where
  sessionized_events.track_id is not null