### Scheduling Notebook Request

#### Requestor To Complete

1. [ ] Specify process name: ``
2. [ ] Create MR to the [data-science/deployments](https://gitlab.com/gitlab-data/data-science/-/tree/main/deployments) folder
   1. The MR should create a new folder containing all of the queries required, along with the Notebook. The [pte folder](https://gitlab.com/gitlab-data/data-science/-/tree/main/deployments/pte) can be used as an example
   2. If any parameters are required for running the notebook please specify them here: `` 
3. [ ] Specify requested schedule: `` 
4. [ ] Ping @data-team/engineers and request the below process


#### Data Engineer To

1. [ ] Create MR for new DAG in analytics repo under [/dags/data_science](https://gitlab.com/gitlab-data/analytics/-/blob/master/dags/data_science) folder.
   1. The [`propensity_to_expand`](https://gitlab.com/gitlab-data/analytics/-/blob/master/dags/data_science/propensity_to_expand.py) DAG can be used as a template. 
   2. Only updates / changes should be for parameters specified above. 
      - Ensure the below fields are updated 
      1. [ ] Name
      2. [ ] Schedule 
      3. [ ] Path 
      4. [ ] Parameters 
