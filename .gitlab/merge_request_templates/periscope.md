## Periscope Dashboard Checklist

<!--
Please complete all items. Ask questions in the #data slack channel
--->

**Dashboard Link**:
`WIP:` should be in the title and it should be in the `WIP` topic

**Dashboard Name**:

**Original Issue Link**:
<!--
If none, please include a description
--->

**Editor Slack Handle**: @`handle`

### DRI/Prioritization Owner Checklist

The DRI/prioritization owner can be found [here](https://about.gitlab.com/handbook/business-ops/data-team/#data-support-per-organization)

* Review Items
   * [ ] Does the dashboard provide the data requested?
   * [ ] Is the data in the dashboard correct?
   * [ ] Does the data align with the existing single source of truth and across applicable reporting in Periscope and Google Sheets?
   * [ ] Is this a new dashboard or a re-evaluation of a dashboard change? 

### Submitter Checklist
* Review Items
   * [ ] SQL formatted using [GitLab Style Guide](https://about.gitlab.com/handbook/business-ops/data-team/sql-style-guide/)
   * [ ] Python / R reviewed for content, formatting, and necessity
   * [ ] Filters, if relevant
   * [ ] Current month (in-progress) numbers and historical numbers are in separate charts
   * [ ] Drill Down Linked, if relevant
   * [ ] Overview/KPI/Top Level Metrics cross-linked
   * [ ] Section Label before more granular metrics
   * [ ] Topics added
   * [ ] Permissions reviewed
   * [ ] Viz Titles changed to Autofit, if relevant
   * [ ] Axes labeled, if relevant
   * [ ] Numbers (Currencies, Percents, Decimal Places, etc) cleaned, if relevant
   * [ ] If using a date filter, set an appropriate length. Most common is 365 days.
   * [ ] Chart description for each chart, linking to Metrics definitions where possible
   * [ ] Legend is clear
   * [ ] Text Tile for "What am I looking at?" and more detailed information, leveraging hyperlinks instead of URLs
   * [ ] Tooltips are used where appropriate and show relevant values
   * [ ] Request approval from stakeholder if applicable
   * [ ] Assign to reviewer on the data team
   * [ ] Make it clear in the issue description that this is a re-evaluation if this is not a new dashboard.  
   * [ ] If it is a dashboard re-evaluation, please add into the dashboard title "WIP". 


* Housekeeping
  - [ ] Assigned to a member of the data team
  - [ ] Allocated to milestone per review time request
  - [ ] Labels and Points Allocated

### Reviewer Checklist
* Review Items
   * [ ] If dashboard, re-evaluation, first take off official badge. 
   * [ ] SQL formatted using [GitLab Style Guide](https://about.gitlab.com/handbook/business-ops/data-team/sql-style-guide/)
   * [ ] Python / R reviewed for content, formatting, and necessity
   * [ ] Filters, if relevant
   * [ ] Current month (in-progress) numbers and historical numbers are in separate charts
   * [ ] Drill Down Linked, if relevant
   * [ ] Overview/KPI/Top Level Metrics cross-linked
   * [ ] Section Label before more granular metrics
   * [ ] Topics added
   * [ ] Permissions reviewed
   * [ ] Viz Titles changed to Autofit, if relevant
   * [ ] Axes labeled, if relevant
   * [ ] Numbers (Currencies, Percents, Decimal Places, etc) cleaned, if relevant
   * [ ] If using a date filter, set an appropriate length. Most common is 365 days.
   * [ ] Chart description for each chart, linking to Metrics definitions where possible
   * [ ] Legend is clear
   * [ ] Text Tile for "What am I looking at?" and more detailed information, leveraging hyperlinks instead of URLs
   * [ ] Tooltips are used where appropriate and show relevant values
   * [ ] If this is a dashboard re-evaluation, check with the business logic owners if the new definitions are correct. 
   * [ ] Remove `WIP:` from title
   * [ ] Remove from `WIP` topic
   * [ ] Add approval badge
   * [ ] Request approval from stakeholder/business partner if applicable
   * [ ] Assign back to submitter for closing or merge if good to go

### Submitter
   * [ ] Post in #data channel in Slack
   * [ ] Link to Periscope Charts/Dashboards from the Handbook where appropriate. 
   * [ ] Close this MR

/label ~Reporting ~Periscope ~Review