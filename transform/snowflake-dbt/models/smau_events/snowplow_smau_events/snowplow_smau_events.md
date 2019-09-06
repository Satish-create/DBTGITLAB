{% docs create_snowplow_smau_events %}

This model encapsulates all activation events for stage create as defined in this 2 gitlab issues (here)[] and (here)[]. It reconciles 2 different data sources (Snowplow and Gitlab) with some common enabling us to calculate Daily/Monthly Active User count for this specific stage.

For more documentation on which event is tracked by each data source for this stage, refer to the 2 upstream models ((snowplow_create_activation_events)[] and (gitlab_create_activation_events)[])
 
{% enddocs %}
