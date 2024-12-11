### stg_page_views

{% docs stg_page_views %}

This model is a sanitized version of the raw pageview data.
{% enddocs %}

### stg_users_map

{% docs stg_users_map %}

This model performs "user stitching" on top of clickstream data. User stitching is the process of tying all events associated with a cookie to the same user_id, and solves a common problem in event analytics that users are only identified part way through their activity stream. This model returns a single user_id for every anonymous_id, and is later joined in to build a `blended_user_id` field, that acts as the primary user identifier for all sessions.

{% enddocs %}