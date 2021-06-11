{% docs subscription_product_usage_data %}

This model collates a variety of product usage data metrics at the subscription_id granularity for both self-managed and SaaS subscriptions. Detailed documentation on the creation of this model, constraints, and example queries can be found on the [Master Subscription Product Usage Data Process Dashboard](https://app.periscopedata.com/app/gitlab/686439/Master-Subscription-Product-Usage-Data-Process).

{% enddocs %}

{% docs mart_product_usage_wave_1_3_metrics_latest %}

The purpose of this mart table is to act as a data pump of the _most recently received_ **Self-Managed** customer product data into Gainsight for Customer Product Insights.

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs mart_product_usage_wave_1_3_metrics_monthly %}

The purpose of this mart table is to act as a data pump of **Self-Managed** customer product usage data at a _monthly grain_ into Gainsight for Customer Product Insights.

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs mart_product_usage_wave_1_3_metrics_monthly_diff %}

The purpose of this mart table is to act as a data pump of the `_all_time_event` Usage Ping metrics at a _monthly grain_ into Gainsight for Customer Product Insights (`_all_time_user` columns are not included). To accomplish this goal, this model includes a column that takes the _diff_ erences in `_all_time_event` values between consecutive monthly Usage Pings. Since some months do not contain Usage Ping data, these _diff_ erences are normalized (estimated) to a monthly value based on the average daily value over the time between pings multiplied by the days in the calendar month(s) between the consecutive pings.

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs mart_saas_product_usage_metrics_monthly %}

The purpose of this mart table is to act as a data pump of **SaaS** customer product usage data at a _monthly grain_ into Gainsight for Customer Product Insights.

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs pump_product_usage_free_user_metrics_monthly %}
This table unions the sets of all Self-Managed and SaaS **free users**. The data from this table will be used for Gainsight Customer Product Insights.

The grain of this table is namespace || uuid-hostname per month.

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs pump_hash_marketing_contact %}

a copy of mart_marketing_contact with the email hashed

{% enddocs %}