with base_import_1 as (
    SELECT
distinct 

CASE
    WHEN REGEXP_CONTAINS(date, r'^\d{4}-\d{2}-\d{2} \d{1,2}:\d{2}:\d{2}$') THEN PARSE_DATE('%Y-%m-%d', SPLIT(date, ' ')[OFFSET(0)])
    WHEN REGEXP_CONTAINS(date, r'^\d{1,2}-\d{1,2}-\d{4} \d{1,2}:\d{2}:\d{2}$') THEN PARSE_DATE('%d-%m-%Y', SPLIT(date, ' ')[OFFSET(0)])
    WHEN REGEXP_CONTAINS(date, r'^\d{1,2}/\d{1,2}/\d{4} \d{1,2}:\d{2}:\d{2}$') THEN
      -- Attempt to parse as both mm/dd/yyyy and dd/mm/yyyy, default to mm/dd/yyyy
      CASE
        WHEN CAST(SPLIT(SPLIT(date, ' ')[OFFSET(0)], '/')[OFFSET(0)] AS INT64) > 12 THEN PARSE_DATE('%d/%m/%Y', SPLIT(date, ' ')[OFFSET(0)])
        ELSE PARSE_DATE('%m/%d/%Y', SPLIT(date, ' ')[OFFSET(0)])
      END
    WHEN REGEXP_CONTAINS(date, r'^\d{1,2}/\d{1,2}/\d{4}$') THEN
      -- Attempt to parse as both mm/dd/yyyy and dd/mm/yyyy, default to mm/dd/yyyy
      CASE
        WHEN CAST(SPLIT(date, '/')[OFFSET(0)] AS INT64) > 12 THEN PARSE_DATE('%d/%m/%Y', date)
        ELSE PARSE_DATE('%m/%d/%Y', date)
      END
    ELSE NULL
  END as date,
  name, email, phone, campaign_id, source, medium

FROM {{source('manual_imports', 'historical_head_boards')}}
),


base_grouped as (
    select min(timestamp(date)) as date, email, max(phone) as phone, max(campaign_id) as campaign_id, max(source) as source, max(medium) as medium, 'https://go.head-boards.nl/historical_data' as  context_page_url
    from base_import_1

    group by email

    union all select min(timestamp(date)) as date, email, max(phone) as phone, max(campaign_id) as campaign_id, max(source) as source, max(medium) as medium, 'https://go.head-boards.nl/historical_data' as  context_page_url
    from {{source('manual_imports', 'missed_typeform_head_boards')}}
    group by email
)

SELECT
  email AS anonymous_id,
  email AS user_id,
  'unknown_city' AS geo_city,
  'unknown_country' AS geo_country,
  'unknown_region' AS geo_region,
  '0.0.0.0' AS ip_address,
  'unknown_resolution' AS screen_screen_resolution,
  'unknown_size' AS screen_viewport_size,
  'unknown_agent' AS user_agent,
  'unknown_browser' AS device_browser_brand,
  'false' AS device_is_mobile,
  'unkown' AS device_platform,
  '/historical_data' AS page_path,
  'go.head-boards.nl/historical_data' AS page_path_host,
  'facebook' AS page_referrer,
  'facebook.com' AS page_referrer_host,
  '' AS page_query,
  'historical_data' AS page_title,
  'https://go.head-boards.nl/historical_data' AS page_url,
  'default.host' AS page_host,
  'facebook' AS utm_source,
  'cpc' AS utm_medium,
  campaign_id AS utm_campaign,
  'unknown_term' AS utm_term,
  'unknown_content' AS utm_content,
  campaign_id AS utm_id,
 {{ get_all_click_ids("context_page_url") }},
  date AS page_view_timestamp,
  {{ dbt_utils.generate_surrogate_key(["email", "date", "campaign_id","phone"])}} as page_view_id,
  'cpc' AS referrer_medium,
  'facebook' AS referrer_source,
  'facebook' AS channel_source,
  'social' AS channel_medium
FROM base_grouped



