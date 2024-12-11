{% macro escape_temporary_utm_adjustment(context_campaign_source,
    context_campaign_medium,
    context_campaign_campaign,
    context_campaign_term,
    context_campaign_content,
    context_page_url) %}

  CASE
    WHEN {{ context_campaign_source }} = 'klaviyo' THEN 'email'
    WHEN {{ context_campaign_source }} = 'facebook' AND {{ context_campaign_medium }} = 'paid' THEN 'facebook'
    WHEN {{ context_campaign_source }} = 'facebook-funnel' THEN 'facebook'
    WHEN {{ context_campaign_source }} = 'email-funnel' THEN 'email'
    WHEN {{ context_campaign_source }} IS NULL OR {{ context_campaign_source }} = '' THEN 
      CASE
        WHEN {{ context_page_url }} LIKE '%wbraid%' OR {{ context_page_url }} LIKE '%gbraid%' OR {{ context_page_url }} LIKE '%gclid%' THEN 'google'
        WHEN {{ context_page_url }} LIKE '%epik%' OR {{ context_page_url }} LIKE '%pp%' THEN 'pinterest'
        WHEN {{ context_page_url }} LIKE '%fbclid%' THEN 'facebook'
        WHEN {{ context_page_url }} LIKE '%_kx%' THEN 'klaviyo'
        WHEN {{ context_page_url }} LIKE '%tw_source%' THEN 'twitter'
        ELSE {{ context_campaign_source }}
      END
    ELSE lower({{ context_campaign_source }})
  END as utm_source,
  CASE
    WHEN {{ context_campaign_source }} = 'klaviyo' THEN 'email'
    WHEN {{ context_campaign_source }} = 'facebook' AND {{ context_campaign_medium }} = 'paid' THEN 'cpc'
    WHEN {{ context_campaign_source }} = 'facebook-funnel' THEN 'cpc'
    WHEN {{ context_campaign_source }} = 'email-funnel' THEN 'cpc'
    WHEN {{ context_campaign_source }} IS NULL OR {{ context_campaign_source }} = '' THEN 
      CASE
        WHEN {{ context_page_url }} LIKE '%wbraid%' OR {{ context_page_url }} LIKE '%gbraid%' OR {{ context_page_url }} LIKE '%gclid%' THEN 'cpc'
        WHEN {{ context_page_url }} LIKE '%epik%' OR {{ context_page_url }} LIKE '%pp%' THEN 'cpc'
        WHEN {{ context_page_url }} LIKE '%fbclid%' THEN 'cpc'
        WHEN {{ context_page_url }} LIKE '%_kx%' THEN 'email'
        WHEN {{ context_page_url }} LIKE '%tw_source%' THEN 'social'
        ELSE lower({{ context_campaign_medium }})
      END
    ELSE {{ context_campaign_medium }}
  END as utm_medium,
  {{ context_campaign_campaign }} as utm_campaign,
  {{ context_campaign_term }} as utm_term,
  {{ context_campaign_content }} as utm_content

{% endmacro %}
