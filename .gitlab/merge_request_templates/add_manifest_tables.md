Closes

* List the tables added/changed below
* Add list of tables to reconcile process.
* Run the `clone_raw_postgres_pipeline` CI job
* Run the `pgp_test` or `gitlab_ops_pgp_test` CI job by right clicking on the job name and opening in a new tab
  * For `pgp_test`, include the `MANIFEST_NAME` variable and input the name of the db (i.e. `gitlab_com`, `customers`, etc.)
  * If this is a SCD table be sure to include:
    * `advanced_metadata: true` in the manifest
    * `TASK_INSTANCE` variable in job trigger with any value (i.e. `mr-2112`)

#### Tables Changed/Added

* [ ] List

* [ ] Add list of tables to reconcile process.

#### PGP Test CI job passed?

* [ ] List
