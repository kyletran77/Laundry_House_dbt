{% docs fct_form_submitted %}

This DBT model, `fct_form_submitted`, consolidates form submission data from various sources to create a unified view. It specifically includes data from a GTM server-side source and other form providers, such as GoHighLevel, ensuring comprehensive coverage of form submissions.

Key Components:
1. `server_side_gtm_form_model`: References the GTM server-side form submissions, particularly from 'stg_form_submitted_gtm_server_side'.

2. `form_provider_models`: An array of DBT model references that represent different form providers. Currently, it includes models like 'stg_form_submitted_gohighlevel'.

3. `form_column_names`: Specifies the columns included in the output, such as 'user_id', 'event_timestamp', 'form_name', and 'submission_source'.

Process:
- `form_provider_form_names`: A CTE that collects distinct form names from specified form provider models.
- `gtm_server_side_excluded`: Filters out records from `gtm_server_side` where `form_name` overlaps with those in the form provider models.
- `combined_data`: Unions the filtered GTM server-side data with the data from all form provider models.

Output:
The final output is a consolidated table containing columns for `user_id`, `event_timestamp`, `form_name`, and `submission_source`. This table offers a holistic view of form submissions across different platforms, while excluding duplicate entries from the GTM server-side source.

Use Case:
Ideal for analytics and reporting, this model provides an essential view of form submissions, facilitating insights into user interactions across various platforms. It can be further utilized in data transformations, advanced analytics models, or direct reporting.

{% enddocs %}
