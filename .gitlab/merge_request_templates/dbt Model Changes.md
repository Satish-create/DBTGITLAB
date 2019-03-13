## Issue

Link the Issue this MR closes

## Solution

Describe the solution.

### Related Links

Please include links to any related MRs and/or issues.

## Submitter Checklist

- [ ] This MR follows the coding conventions laid out in the [style guide](https://gitlab.com/meltano/meltano#dbt-coding-conventions)

#### Structure
- [ ] Model-specific attributes (like sort/dist keys) should be specified in the model
- [ ] Only base models are used to reference source tables/views
- [ ] All `{{ ref('...') }}` statements should be placed in CTEs at the top of the file

#### Style
- [ ] Field names should all be lowercased
- [ ] Function names should all be capitalized
- [ ] This MR contains new macros
  - [ ] New macros follow the naming convention (file name matches macro name) 
  - [ ] New macros have been documented in the macro README

#### Testing
- [ ] Every model should be tested AND documented in a `schema.yml` file. At minimum, unique, not nullable fields, and foreign key constraints should be tested, if applicable ([Docs](https://docs.getdbt.com/docs/testing-and-documentation))
- [ ] The output of dbt test should be pasted into MRs directly below this point

<details>
<summary> dbt test results </summary>

<pre><code>

Paste the results of dbt test here, including the command.

</code></pre>
</details>

## All MRs Checklist
* [ ]  [Label hygiene](https://about.gitlab.com/handbook/business-ops/data-team/#issue-labeling) on issue
* [ ]  Pipelines pass
* [ ]  This MR is ready for final review and merge.
* [ ]  Assigned to reviewer

## Reviewer Checklist
* [ ]  Check before setting to merge

## Further changes requested
* [ ]  AUTHOR: Uncheck all boxes before taking further action. 
