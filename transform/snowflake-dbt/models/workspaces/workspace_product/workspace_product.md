{% docs fct_usage_event %}

Type of Data: gitlab.com db usage events
Aggregate Grain: Event
Use case: Source of truth (atomic), contains foreign keys to easily join to DIM tables or other FCT/MART tables for additional detail and discovery

{% enddocs %}


{% docs mart_usage_event %}

Type of Data: gitlab.com db usage events
Aggregate Grain: Event (this would be the mart version of the atomic table)
Time Grain: None
Use case: Everyday analysis and dashboards; flexibility in aggregating by sets of events, different time ranges, exclude specific projects

{% enddocs %}


{% docs mart_xmau_metric_monthly %}

Type of Data: gitlab.com db usage events
Aggregate Grain: reporting_month, user_group, section_name, stage_name, and group_name
Time Grain: Last 28 days of the month
Use case: Paid SaaS xMAU, SaaS SpO
Usage is attributed to a namespace's last reported plan (free vs paid)
(GitLab.com mart that determines unique namespace and user counts for total, free and paid metrics.)

{% enddocs %}

{% docs mart_usage_event_plan_monthly %}

Type of Data: gitlab.com db usage events
Aggregate Grain: reporting_month, plan_id_at_event_date, and event_name
Time Grain: Last 28 days of the month
Use case: Paid SaaS xMAU, SaaS SpO
(Trying to mimic Self-managed usage ping format by getting the count of each event that happened in the last 28 days.
Trying to mimic instance-level service ping format by getting the count of each event that happened in the last 28 days of the month.

{% enddocs %}

{% docs mart_usage_namespace_daily %}

Type of Data: gitlab.com db usage events
Aggregate Grain: event_date, event_name, and dim_namespace_id
Time Grain: Day
Use case: everyday analysis and dashboards; flexibility in aggregating by sets of events, different time ranges

{% enddocs %}


{% docs mart_usage_instance_daily %}

Type of Data: gitlab.com db usage events
Aggregate Grain: event_date, event_name, dim_instance_id (all od SaaS)
Time Grain: Day
Use case: everyday analysis and dashboards; flexibility in aggregating by sets of events, different time ranges

{% enddocs %}


{% docs mart_usage_event_daily %}

Type of Data: gitlab.com db usage events
Aggregate Grain: event_date, dim_user_id, dim_namespace_id, and event_name
Time Grain: Day
Use case: everyday analysis and dashboards; flexibility in aggregating by sets of events, different time ranges

{% enddocs %}

{% docs fct_service_ping_instance_metric %}

Type of Data: Version app
Aggregate Grain: dim_service_ping_instance_id and flattened_high_level.metrics_path
Time Grain: None
Use case: Self managed usage data fact table

{% enddocs %}

{% docs dim_service_ping_instance %}

Type of Data: Version app
Aggregate Grain: dim_service_ping_instance_id
Time Grain: None
Use case: Self managed usage data dim table

{% enddocs %}

{% docs mart_service_ping_instance_metric %}

Type of Data: Version app
Aggregate Grain: dim_service_ping_instance_id and metrics_path
Time Grain: None
Use case: Self managed usage data mart table

{% enddocs %}

{% docs mart_service_ping_instance_metric_28_day %}

Type of Data: Version app
Aggregate Grain: dim_service_ping_instance_id and metrics_path
Time Grain: 28 day time frame
Use case: Self managed usage data mart table

{% enddocs %}

{% docs mart_service_ping_instance_metric_7_day %}

Type of Data: Version app
Aggregate Grain: dim_service_ping_instance_id and metrics_path
Time Grain: 7 day time frame
Use case: Self managed usage data mart table

{% enddocs %}

{% docs mart_service_ping_instance_metric_all_time %}

Type of Data: Version app
Aggregate Grain: dim_service_ping_instance_id and metrics_path
Time Grain: 'all' time frame
Use case: Self managed usage data mart table

{% enddocs %}

{% docs rpt_service_ping_instance_metric_adoption_monthly %}


Type of Data: Version app
Aggregate Grain: reporting_month, metrics_path, and estimation_grain
Time Grain: None
Use case: Model used to determine active seats and subscriptions reporting on any given metric


{% enddocs %}

{% docs rpt_service_ping_instance_metric_estimated_monthly %}

Type of Data: Version app
Aggregate Grain: reporting_month, metrics_path, estimation_grain, ping_edition_product_tier, and service_ping_delivery_type
Time Grain: None
Use case: Model used to estimate usage based upon reported and unreported seats/subscriptions for any given metric.

{% enddocs %}

{% docs prep_service_ping_instance %}

Type of Data: Version app
Aggregate Grain: dim_service_ping_instance_id
Time Grain: None
Use case: Self managed usage data prep table

{% enddocs %}

{% docs prep_service_ping_instance_flattened %}

Type of Data: Version app
Aggregate Grain: dim_service_ping_instance_id and path
Time Grain: None
Use case: Self managed usage data prep table

{% enddocs %}

{% docs mart_service_ping_estimations_monthly %}

Estimation model to estimate the usage for unreported self-managed instances.

{% enddocs %}

{% docs dim_service_ping_metric %}

New dimension table to replace dim_usage_ping_metric with some enhancements: surrogate key, cleaning of groups, and renamed.
{% enddocs %}
