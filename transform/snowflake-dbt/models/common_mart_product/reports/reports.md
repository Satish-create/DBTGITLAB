{% docs rpt_event_xmau_metric_monthly %}

Reporting model that calculates unique user and namespace counts for GitLab.com xMAU metrics.

Type of Data: `gitlab.com db usage events`

Aggregate Grain: `user_group (total, free, paid), section_name, stage_name, and group_name`

Time Grain: `reporting_month (defined as the last 28 days of the calendar month). This is intended to match the instance-level service ping metrics by getting a 28-day count of each event.`

Use case: `Paid SaaS xMAU, SaaS SpO`

Note: Usage is attributed to a namespace's last reported plan (free vs paid)
{% enddocs %}


{% docs rpt_event_plan_monthly %}

Type of Data: `gitlab.com db usage events`

Aggregate Grain: `plan_id_at_event_date, event_name`

Time Grain: `reporting_month (defined as the last 28 days of the calendar month). This is intended to match the instance-level service ping metrics by getting a 28-day count of each event.`

Use case: Paid SaaS xMAU, SaaS SpO

{% enddocs %}


{% docs rpt_ping_counter_statistics %}

Model to explore statistics around usage ping counters. 

This includes the following statistics:

  * first version
  * first major version
  * first minor version
  * last version
  * last major version
  * last minor version
{% enddocs %}


{% docs rpt_ping_instance_active_subscriptions %}

Model used to determine active subscriptions.

{% enddocs %}


{% docs rpt_ping_instance_metric_adoption_monthly_all %}

Type of Data: `Version app`

Aggregate Grain: `reporting_month, metrics_path, and estimation_grain`

Time Grain: `None`

Use case: `Model used to determine active seats and subscriptions reporting on any given metric`

{% enddocs %}


{% docs rpt_ping_instance_metric_adoption_subscription_monthly %}

Model used to determine active seats and subscriptions reporting on any given metric.

{% enddocs %}


{% docs rpt_ping_instance_metric_adoption_subscription_metric_monthly %}

Model used to determine active seats and subscriptions reporting on any given metric.

{% enddocs %}


{% docs rpt_ping_instance_metric_estimated_monthly %}

Type of Data: `Version app`

Aggregate Grain: `reporting_month, metrics_path, estimation_grain, ping_edition_product_tier, and service_ping_delivery_type`

Time Grain: `None`

Use case: `Model used to estimate usage based upon reported and unreported seats/subscriptions for any given metric.`

{% enddocs %}


{% docs rpt_ping_instance_subscription_opt_in_monthly %}

Monthly counts of active subscriptions.

{% enddocs %}


{% docs rpt_ping_instance_subscription_metric_opt_in_monthly %}

Monthly counts of active subscriptions.

{% enddocs %}
