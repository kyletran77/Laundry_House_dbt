with segment_events_combined as (
-- combine all enabled events tables into one combined events tables
select
  track_events.event_timestamp as timestamp,
  user_id,
  anonymous_id,
  track_id,
  null as page_view_id
from 
  {{ref('stg_tracks_sgtm')}} as track_events
union all 
select
  page_events.page_view_timestamp as timestamp,
  user_id,
  anonymous_id,
  null as track_id,
  page_view_id
from 
  {{ref('stg_page_views')}} as page_events
),

segment_events_mapped as (
-- map anonymous_id to user_id (where possible)
select
  segment_events_combined.timestamp,
  anonymous_id,

  coalesce(
    segment_events_combined.user_id,
    segment_user_anonymous_map.user_id,
    segment_events_combined.anonymous_id
  ) as blended_user_id,
  track_id,
  page_view_id
from
  segment_events_combined
  left join {{ref('stg_users_map')}} as segment_user_anonymous_map
   using(anonymous_id)
),

session_starts as (
-- label the event that starts the session
select
  *,
  coalesce(
    (
      timestamp_diff(
    segment_events_mapped.timestamp , lag(timestamp ) over (partition by blended_user_id order by timestamp asc ), millisecond)
    ) >= 1800000,
    true
  ) as session_start_event
from
  segment_events_mapped
),

with_session_index as (
-- add a session_index (users first session = 1, users second session = 2 etc)
select
  *,
  sum(case when session_start_event then 1 else 0 end ) over (partition by blended_user_id order by session_starts.timestamp asc rows between unbounded preceding and current row) as session_index
from
  session_starts
)

-- add a unique session_id to each session
select
  *,
  cast(farm_fingerprint(concat(cast(session_index as string),cast(blended_user_id as string))) as string) as session_id
from
  with_session_index