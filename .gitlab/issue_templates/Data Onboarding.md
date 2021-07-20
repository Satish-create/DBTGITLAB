## Overview

Goal: To help bring you, our new data team member, up to speed in the GitLab Data Team's analytics stack as efficiently as possible, without sacrificing quality for speed. There is a lot of information in the on-boarding issue, so please bookmark handbook pages, documentation pages, and log-ins for future reference. The goal is for you to complete and close the Data Team on-boarding issue within 1 week after you have completed the GitLab company on-boarding issue. These resources will be super helpful and serve as great reference material as you get up to speed and learn to work through issues and merge requests [over your first 90 days](https://gitlab.com/gitlab-com/www-gitlab-com/blob/master/source/job-families/finance/data-analyst/index.html.md#how-youll-ramp).


## Access Requests

### For all going through the Data Onboarding Process
- [ ] Manager: Upgrade Periscope/Sisense user to editor (after they've logged in via Okta)
- [ ] Manager: Add to Snowflake [following Handbook Process](https://about.gitlab.com/handbook/business-ops/data-team/platform/#warehouse-access)
- [ ] Manager: Add to Data Team calendar 
- [ ] Manager: Add to the `GitLab Data Team` project as a Developer.
- [ ] Manager: Customize this template for the analysts specialty, if any. Delete sections, if appropriate
- [ ] Manager: Add to Lucidchart via Okta
- [ ] ManageR: Add to Lucidchart `Data Team` team folder

### For Central or Embedded Analyst/Engineers
- [ ] Manager: Create access request 
   - [ ] Manager: Request addition to `@datateam` alias on Slack in PeopleOps Onboarding issue
   - [ ] Manager: Request addition to `@data-analysts` alias on Slack in PeopleOps Onboarding issue
   - [ ] Manager: Request addition to `Data Team` 1password vault in PeopleOps Onboarding issue
   - [ ] Join the following channels on Slack: `data`, `data-lounge`, `data-daily`, `data-triage`, and `enterprise-apps`.
   - [ ] Engineers, join `analytics-pipelines` and `data-prom-alerts`
   - [ ] Analysts & Engineers, join `dbt-runs`
- [ ] Manager: Add to the `gitlab-data` namespace as a Developer.
- [ ] Manager: Add to Airflow as Analyst
- [ ] Manager: Update codeowners file in the handbook to include the new team member
- [ ] Manager: Add to daily Geekbot standup (send `dashboard` to Geekbot on slack, click into a particular standup in the web UI, add via Manage button)
- [ ] Manager: Invite to SheetLoad & Boneyard folders in Google Drive
- [ ] Manager: Add to data team calendar as a calendar admin
- [ ] Manager: Add team member to Finance team meetings
- [ ] Manager: Add to [data triage](https://about.gitlab.com/handbook/business-ops/data-team/how-we-work/duties/#triager) in third week at GitLab (Week 1 = Company onboarding; Week 2 = Data team onboarding)
- [ ] Manager: Update issue with one or two Good First Issues
- [ ] Manager: Customize this template for the analysts specialty, if any. Delete sections, if appropriate

### For Engineers
- [ ] Manager: Add to `gitlab-data/gitlag-data-engineers` group as Developer
- [ ] Manager: Request addition to `@data-engineers` alias on Slack in PeopleOps Onboarding issue
- [ ] Manager: Add to `gitlab-analytics` GCP group
    - [ ] Manager: Provision service account credentials in GCP
- [ ] Manager: Add to Stitch
- [ ] Manager: Add to Fivetran.(For enabling Fivetran in Okta [use google groups](https://about.gitlab.com/handbook/business-ops/okta/#managing-okta-access-using-google-groups)) 
- [ ] Manager: Add to Airflow as Admin

## WELCOME TO THE TEAM! WE'RE SO EXCITED TO HAVE YOU!!!

- [ ] Read (skim) through this full issue, just so you have a sense of what's coming.
- [ ] Create a new issue in the Analytics project (this project). As you proceed and things are unclear, document it in the issue. Don't worry about organizing it; just brain dump it into the issue! This will help us iterate on the onboarding process.
- [ ] Join the following channels on Slack: `data`, `data-lounge`, `data-daily`, `data-triage`, and `business-technology`.
   - [ ] Engineers, join `analytics-pipelines`
- [ ] Schedule a recurring fortnightly (every two weeks) 1:1 meeting with the Sr. Director of Data and Analytics.
- [ ] Schedule a coffee chat with each member of the data team. These should be in addition to the ones you do with other GitLab team members. Consider making these recurring meetings for every 3-4 weeks with everyone you will work closely with. In addition, you should also consider scheduling chats with Business Technology (IT, Enterprise Apps, Procurement) people as well.
- [ ] Read the following pages of the handbook in their entirety. Bookmark them as you should soon be making MR's to improve our documentation!
   - [ ] [Data Team](https://about.gitlab.com/handbook/business-ops/data-team/)
   - [ ] [Business Operations](https://about.gitlab.com/handbook/business-ops/)
   - [ ] [Data Quality Process](https://about.gitlab.com/handbook/business-ops/data-team/data-quality/)
   - [ ] [Periscope Directory](https://about.gitlab.com/handbook/business-ops/data-team/platform/periscope/)
- [ ] Watch @tlapiana's [talk at DataEngConf](https://www.youtube.com/watch?v=eu623QBwakc) that gives a phenomenal overview of how the team works.
- [ ] Watch [this great talk](https://www.youtube.com/watch?v=prcz0ubTAAg) on what Analytics is
- [ ] If relevant, watch ["The State of [Product] Data"](https://www.youtube.com/watch?v=eNLkj3Ho2bk&feature=youtu.be) from Eli at the Growth Fastboot. (You'll need to be logged into GitLab Unfiltered.)
There is a lot of information being thrown at you over the last couple of days.
It can all feel a bit overwhelming.
The way we work at GitLab is unique and can be the hardest part of coming on board.
It is really important to internalize that we work **handbook-first** and that **everything is always a work in progress**.
Please watch one minute of [this clip](https://www.youtube.com/watch?v=LqzDY76Q8Eo&feature=youtu.be&t=7511) (you will need to be logged into GitLab unfiltered) where Sid gives a great example of why its important that we work this way.
*This is the most important thing to learn during all of onboarding.*

**Getting your computer set up locally**
* Make sure that you have [created your SSH keys](https://docs.gitlab.com/ee/gitlab-basics/create-your-ssh-keys.html) prior to running this. You can check this by typing `ssh -T git@gitlab.com` into your terminal which should return "Welcome to GitLab, " + your_username. Make the SSH key with no password.
<details>

<summary>For Data Analysts</summary>

* THE NEXT STEPS SHOULD ONLY BE RUN ON YOUR GITLAB-ISSUED LAPTOP. If you run this on your personal computer, we take no responsibility for the side effects.

* [ ] Open your computer's built-in terminal app. Run the following:
```
curl https://gitlab.com/gitlab-data/analytics/raw/master/admin/onboarding_script.zsh > ~/onboarding_script.zsh
zsh ~/onboarding_script.zsh
rm ~/onboarding_script.zsh
```
   * This script is written for zsh, the default terminal for MacOS now, if you feel strongly that you prefer or would like to keep bash, please see this [commit & script](https://gitlab.com/gitlab-data/analytics/-/blob/6964ba11c46c0a3caf863c8fae0b89ba24bb3c48/admin/onboarding_script.sh)
      * However, this script is no longer actively maintained or supported, so you will need to support yourself if you do this. If you do use bash it is also recommended to compare the script with the latest zsh version to make sure you aren't missing any new apps or tools that have been added.
   * This may take a while, and it might ask you for your password (multiple times) before it's done. Here's what this does:
      * Installs iTerm, a mac-OS terminal replacement
      * Installs VSCode, an open source text editor. VSCode is recommended for multiple reasons including community support, the [GitLab workflow](https://marketplace.visualstudio.com/items?itemName=fatihacet.gitlab-workflow) extension, and the LiveShare features.
      * Installs oh-my-zsh for easy terminal theming, git autocomplete, and a few other plugins. If you are curious or would like to change the look and feel of your shell please [go here](https://github.com/ohmyzsh/ohmyzsh).
      * Installing dbt, the open source tool we use for data transformations.
      * Installing jump, an easy way to move through the file system. [Please find here more details on how to use jump](https://github.com/ohmyzsh/ohmyzsh/tree/master/plugins/jump)
      * Installing anaconda, how we recommend folks get their python distribution.
      * Installs all-the-things needed to contribute to [the handbook](about.gitlab.com/handbook) locally and build it locally.
   * You will be able to `jump analytics` from anywhere to go to the analytics repo locally (you will have to open a new terminal window for `jump` to start working.) If it doesn't work, try running `cd ~/repos/analytics`. Once in "analytics" folder run command `mark analytics` then quit + reopen your terminal before trying again. Now path ~/repos/analytics has been named "analytics" and you can enter to it by using command `mark analytics`.
   * You will be able to `gl_open` from anywhere within the analytics project to open the repo in the UI. If doesn't work, visually inspect your `~/.bashrc` file to make sure it has [this line](https://gitlab.com/gitlab-data/analytics/blob/master/admin/make_life_easier.sh#L14).
   * Your default python version should now be python 3. Typing `which python` into a new terminal window should now return `/usr/local/anaconda3/bin/python`
   * dbt will be installed at its latest version. Typing `dbt --version` will output the current version.
   * To get to the handbook project, you'll be able to use `jump handbook`, and to build the handbook locally, you'll be able to use the alias `build_hb!`.
* [ ] Install docker & docker-compose. The easiest way to do this for Mac now is to use the desktop install from [Docker](https://www.docker.com/products/docker-desktop). If working on Linux you will need to follow these [install instructions](https://docs.docker.com/engine/install/ubuntu/) instead. When running on Mac, installing docker only for `dbt` is not needed, since `Venv` is the [recommended](https://about.gitlab.com/handbook/business-technology/data-team/platform/dbt-guide/#Venv-workflow) workflow for anyone running a Mac system.
* [ ] We strongly recommend configuring VSCode (via the VSCode UI) with the [VSCode setup](https://discourse.getdbt.com/t/how-we-set-up-our-computers-for-working-on-dbt-projects/243?) section of Claire's post and [adding the tip](https://discourse.getdbt.com/t/how-we-set-up-our-computers-for-working-on-dbt-projects/243/10?u=tmurphy) from tmurphy later in the thread. It will make your life much easier.
  * Your editor should be configured so that all tabs are converted to 4 spaces. This will minimize messy looking diffs and provide consistency across the team.
    * VSCode
      * `Editor: Detect Indentation` is deselected
      * `Editor: Insert Spaces` is selected
      * `Editor: Tab Size` is set to 4 spaces per tab
* [ ] Consider following [these instructions](https://stackoverflow.com/a/23963086) so you can have option + arrow keys to move around the terminal easier
* [ ] Raise an access request for Google Cloud Platform (GCP). Template for access request (AR) can be found here: https://gitlab.com/gitlab-com/team-member-epics/access-requests/-/issues/10306. Please assign it to your manager
* [ ] If you get a weird semaphore issue error when running dbt try [this script](https://gist.github.com/llbbl/c54f44d028d014514d5d837f64e60bac) which is sourced from this [Apple forum thread](https://forums.developer.apple.com/thread/119429)
* [ ] (Optional) - Better terminal theming - In the onboarding script the terminal has been configured to use the [bira OhMyZsh theme](https://github.com/ohmyzsh/ohmyzsh/wiki/Themes#bira). However if you would like an improved and configurable theme install [PowerLevel10K](https://github.com/romkatv/powerlevel10k) by running the below command from your terminal: 
    ``` 
    git clone --depth=1 https://github.com/romkatv/powerlevel10k.git ${ZSH_CUSTOM:-$HOME/.oh-my-zsh/custom}/themes/powerlevel10k
    ```
    * Then reopen your terminal and you will be asked to configure this theme. If you would like to reconfigure the theme run `p10k configure`
</details>

<details>
<summary>For Data Scientists</summary>

-  [ ] Make sure you have run through all of the analysts steps mentioned above, along with the getting setup with dbt section below

-  [ ] Install docker & docker-compose. The easiest way to do this for Mac now is to use the desktop install from [Docker](https://www.docker.com/products/docker-desktop).

-  [ ] Run through the Jupyter setup section below 
</details>

<details>
<summary>For Data Engineers</summary>

Take a look at https://gitlab.com/gitlab-data/analytics/-/blob/master/admin/onboarding_script.zsh and feel free to use what is in there that makes sense. This script is activily maintained in the system so try to use this. 

Some important parts of the script that you will definitely want to do in some way:
* [ ] Install git
    * [ ] Setup a global gitignore that ignores IDE generated files
    * [ ] Clone the analytics project at git@gitlab.com:gitlab-data/analytics.git
    * [ ] Make sure to globally configure git with at least your name and email.
    * [ ] Ensure you have [git completion](https://stackoverflow.com/questions/24315201/warning-this-script-is-deprecated-please-see-git-completion-zsh/41767727#41767727) configured to make your life easier
* [ ] Install docker & docker-compose. The easiest way to do this for Mac now is to use the desktop install from [Docker](https://www.docker.com/products/docker-desktop). If working on Linux you will need to follow these [install instructions](https://docs.docker.com/engine/install/ubuntu/) instead. 
* [ ] Install Python3. [The Hitchhiker's Guide to Python](https://docs.python-guide.org/starting/install3/osx/) is a good resource
    * [ ] Install pip3
    * [ ] Make sure to install the setuptools library as dbt will not install without it
    * [ ] Here is [a list of all of the Python tools that may be used for formatting, linting, or testing](https://gitlab.com/gitlab-data/analytics/blob/master/.gitlab-ci.yml#L100).  Consider installing these locally with pip3.
* [ ] Install dbt, the open source tool we use for data transformations.
    * [ ] Create a dbt profile file in `~/.dbt/`
    * [ ] Set the DBT_PROFILE_PATH environment variable to point to the profile file
* [ ] Install your Python-compatible IDE of choice.  We recommend VSCode for its community support, [GitLab workflow](https://marketplace.visualstudio.com/items?itemName=fatihacet.gitlab-workflow) extension, and overall flexibility.
    * [ ] Ensure your IDE converts tabs to 4 spaces.  To do that in VSCode, make sure in settings:
      * `Editor: Detect Indentation` is deselected
      * `Editor: Insert Spaces` is selected
      * `Editor: Tab Size` is set to 4 spaces per tab
    * [ ] Ensure your IDE uses the Python3 installation
        * To do this in VSCode: press `Ctrl+Shift+P` and then type in and select `Python: Select Interpreter` and then select the Python 3 installation
    * [ ] Consider installing extensions/add-ons in your IDE to support the Python libraries used for linting/testing as mentioned in the Python section above.  For example, you can setup VSCode to use black as its formatter as described [here](https://code.visualstudio.com/docs/python/editing#_formatting).
    * [ ] Consider installing [tldr](https://tldr.sh/) for easy reference to common CLI commands


Additional tools to install that are not part of the onboarding script:
* [ ] Install Data Grip (from JetBrains) for interfacing with databases
    * [ ] Follow [this process](https://about.gitlab.com/handbook/tools-and-tips/#jetbrains) for requesting a license for Data Grip.  Until you have a license, you can easily use Data Grip on a trial basis for 30 days
    - Change your formatting preferences in Data Grip by going to Preferences > Editor > Code Style > HTML. You should have:
        * Use tab character: unchecked
        * Tab size: 4
        * Indent: 4
        * Continuation indent: 8
        * Keep indents on empty lines: unchecked
    - You can use `Command + Option + L` to format your file.
* [ ] Install the [gcloud sdk](https://cloud.google.com/sdk/docs/quickstart-macos) and authenticate once you're provisioned.
    * [ ] For debugging services such as Airflow locally, you will need a set of service account credentials. Your manager will provide you with a service account.
    * [ ] The environment variable `GOOGLE_APPLICATION_CREDENTIALS` should then point to the key provided by your manager.
* [ ] Install [kubectl](https://kubernetes.io/docs/tasks/tools/install-kubectl/#install-with-homebrew-on-macos)
* [ ] Install the [awscli](https://aws.amazon.com/cli/)
* [ ] Disable [autocorrect in zsh](https://coderwall.com/p/jaoypq/disabling-autocorrect-in-zsh) if it annoys you
* [ ] Consider downloading and installing [Little Snitch](https://www.obdev.at/products/littlesnitch/index.html) - You can submit for reimbursement for the full version
* [ ] (Optional) - Better terminal theming - In the onboarding script the terminal has been configured to use the [bira OhMyZsh theme](https://github.com/ohmyzsh/ohmyzsh/wiki/Themes#bira). However if you would like an improved and configurable theme install [PowerLevel10K](https://github.com/romkatv/powerlevel10k) by running the below command from your terminal: 
    ``` 
    git clone --depth=1 https://github.com/romkatv/powerlevel10k.git ${ZSH_CUSTOM:-$HOME/.oh-my-zsh/custom}/themes/powerlevel10k
    ```
    * Then reopen your terminal and you will be asked to configure this theme. If you would like to reconfigure the theme run `p10k configure`

## Airflow (Data Engineers only)
    - [ ] Read the Airflow section on the [Data Infrastructure page](https://about.gitlab.com/handbook/business-ops/data-team/platform/infrastructure/#airflow)
    - [ ] Watch the [Airflow Setup Walkthrough](https://www.youtube.com/watch?v=3Ym40gRHtvk&feature=youtu.be) with Taylor and Magda


</details>

**Bonus**
To see the inspiration for the onboarding script above, take a look at the dbt Discourse post [here](https://discourse.getdbt.com/t/how-we-set-up-our-computers-for-working-on-dbt-projects/243) on how they set up their computers for working on dbt projects. You might want to do some of the additional configurations mentioned in that post.

## Data stack
On [the Data team handbook page](https://about.gitlab.com/handbook/business-ops/data-team/platform/#extract-and-load), we explain the variety of methods used to extract data from its raw sources (`pipelines`) to load into our Snowflake data warehouse. We use open source dbt (more on this in a moment) as our transformation tool. The bulk of your projects and tasks will be in dbt , so we will spend a lot of time familiarizing yourself with those tools and then dig into specific data sources.
 - [ ] Our current data infrastructure is represented in this [system diagram](https://about.gitlab.com/handbook/business-ops/data-team/platform/infrastructure/#system-diagram)

## Connecting to Snowflake
- [ ] Login with the credentials that your manager created following the instructions at https://about.gitlab.com/handbook/business-ops/data-team/platform/#warehouse-access. Please note that currently Snowflake is accessed through Okta, however you still need to raise access request to get credentials, as you will need to restart your password and update dbt profile with Snowflake credentials. Access request should be raised the same way as it was for Google Cloud Platform credentials.
- [ ] Snowflake has a Web UI for querying the data warehouse that can be found under [Worksheets](https://gitlab.snowflakecomputing.com/console#/internal/worksheet). Familiarize yourself with it. Change your password and update your role, warehouse, and database to the same info you're instructed to put in your dbt profile (Ask your manager if this is confusing or check out [roles.yml](https://gitlab.com/gitlab-data/analytics/blob/master/load/snowflake/roles.yml) to see which roles, warehouses, and databases you've been assigned). The schema does not matter because your query will reference the schema.
- [ ] Run `alter user "your_user" set default_role = "your_role";` to set the UI default Role to your appropriate role instead of `PUBLIC`. (E.g. `alter user "KDIETZ" set default_role = "KDIETZ";`)
- [ ] You can test your Snowflake connection in the UI by first running selecting which warehouse to use (e.g. `use warehouse ANALYST_XS;`), clicking the "play" button, and then querying a database you have access to (e.g. `select * from "PROD"."COMMON"."DIM_CRM_PERSON" limit 10;`) 
- [ ] We STRONGLY recommend using the UI, but if you must download a SQL development tool, you will need one that is compatible with Snowflake, such as [SQLWorkbench/J](http://sql-workbench.net) or [DataGrip](https://www.jetbrains.com/datagrip/). If you're interested in DataGrip, follow the [instructions to get a JetBrains license in the handbook](https://about.gitlab.com/handbook/tools-and-tips/#jetbrains). If using DataGrip, you may need to download the [Driver](https://docs.snowflake.net/manuals/user-guide/jdbc-download.html#downloading-the-driver). This template may be useful as you're configuring the DataGrip connection to Snowflake `jdbc:snowflake://{account:param}.snowflakecomputing.com/?{password}[&db={Database:param}][&warehouse={Warehouse:param}][&role={Role:param}]` We recommend not setting your schema so you can select from the many options. If you do use Data Grip, please set up the following configuration:


#### Snowflake SQL
Snowflake SQL is probably not that different from the dialects of SQL you're already familiar with, but here are a couple of resources to point you in the right direction:
- [ ] [Differences we found while transition from Postgres to Snowflake](https://gitlab.com/gitlab-data/analytics/issues/645)
- [ ] [How Compatible are Redshift and Snowflake’s SQL Syntaxes?](https://medium.com/@jthandy/how-compatible-are-redshift-and-snowflakes-sql-syntaxes-c2103a43ae84)
- [ ] [Snowflake Functions](https://docs.snowflake.net/manuals/sql-reference/functions-all.html)

## dbt

### What is dbt?
- [ ] Familiarize yourself with [dbt](https://www.getdbt.com/) and how we use it by reading our [dbt Guide](https://about.gitlab.com/handbook/business-ops/data-team/platform/dbt-guide/).


<img src = "https://d33wubrfki0l68.cloudfront.net/18774f02c29380c2ca7ed0a6fe06e55f275bf745/a5007/ui/img/svg/product.svg">

- [ ] Refer to http://jinja.pocoo.org/docs/2.10/templates/ as a resource for understanding Jinja which is used extensively in dbt.
- [ ] [This podcast](https://www.dataengineeringpodcast.com/dbt-data-analytics-episode-81/) is a general walkthrough of dbt/interview with its creator, Drew Banin.
- [ ] Read our [SQL Style Guide](https://about.gitlab.com/handbook/business-ops/data-team/platform/sql-style-guide/).
- [ ] Watch [video](https://www.youtube.com/watch?v=P_NQ9qHnsyQ&feature=youtu.be) of Thomas and Israel discussing getting started with dbt locally.
- [ ] Peruse the [Official Docs](https://docs.getdbt.com).
- [ ] In addition to using dbt to manage our transformations, we use dbt to maintain [our own internal documentation](https://dbt.gitlabdata.com) on those data transformations. This is a public link. We suggest bookmarking it.
- [ ] Read about and and watch [Drew demo dbt docs to Emilie & Taylor](https://blog.fishtownanalytics.com/using-dbt-docs-fae6137da3c3). Read about [Scaling Knowledge](https://blog.fishtownanalytics.com/scaling-knowledge-160f9f5a9b6c) and the problem we're trying to solve with our documentation.
- [ ] Consider joining [dbt slack](https://slack.getdbt.com) (Not required, but strongly recommended; if you join use your personal email).
- [ ] Information and troubleshooting on dbt is sparse on Google & Stack Overflow, we recommend the following sources of help when you need it:
   * Your teammates! We are all here to help!
   * dbt slack has a #beginners channel and they are very helpful.
   * [Fishtown Analytics Blog](https://blog.fishtownanalytics.com)
   * [dbt Discourse](http://discourse.getdbt.com)
</details>

### Getting Set up with dbt locally
- Ensure you've set up your SSH configuration in the previous step as this is required to connect to one our dbt packages
- All dbt commands need to be run within the `dbt-image` docker container
- To get into the `dbt-image` docker container, go to the analytics project (which you can get to by typing `jump analytics` from anywhere on your Mac) and run the command `make dbt-image`. This will spin up our docker container that contains `dbt` and give you a bash shell within the `analytics/transform/snowflake-dbt` directory.
- All changes made to the files within the `analytics` repo will automatically be visible in the docker container! This container is only used to run `dbt` commands themselves, not to write SQL or edit `dbt` files in general (though technically it could be, as VIM is available within the container)
- In case you encounter any error, ensure that docker is up and running. This can be done by running `docker run hello-world`. This should print a "Hello" from Docker message.  If it does not print the "hello" message, then docker needs to be launched. On a Mac, docker is launched by running the command `open /Applications/Docker.app`.


- [ ] Setup command "code" in VS studio by using `command` + `shift` + `p` in VS studio and choose "Install 'code' command in PATH command." This will allow you to use word `code` in terminal which will open indicated file directly in Visual Studio. This step is essential to complete next step.
- [ ] From a different terminal window run `code ~/.dbt/profiles.yml` and update this file with your info.  The schema should be something like `yourname_scratch`. See [sample profiles](https://gitlab.com/gitlab-data/analytics/-/blob/master/admin/sample_profiles.yml) for an example.
    - For the `password` field, you will need to request a Snowflake password reset from the Data Engineering team. Your Okta SSO password will not work for this. You can request this by commenting on the snowflake request access issue created for you and tagging the Data Engineering team. After you reset your password, use that password in the `profiles.yml` password field
    - Your `role` maybe the same as your `database`.
    - [ ] __Data Engineers__: update the following paramaters in the `~/.dbt/profiles.yml`:
        ```
        role: ENGINEER
        warehouse: ENGINEER_XS
        ```
- Back in your terminal window running the dbt docker image: 
- [ ] Run `dbt seed` to import the CSV's from the analytics/data into your schema. For dbt to compile this needs to be completed as some of the models have dependencies on the tables which are created by the CSV's.
- [ ] Run `dbt run --models +staging.sfdc` from within the container to know that your connection has been successful, you are in the correct location, and everything will run smoothly.  For more details on the syntax for how to select and run the models, please refer to this [page](https://docs.getdbt.com/reference/node-selection/syntax#examples).  Afterwards, you can also try running `dbt compile` to ensure that the entire project will compile correctly.
- [ ] Run `Exit` command to come out of the dbt docker container and test the command `make help` and use it to understand how to use `make dbt-docs` and access it from your local machine.
- [ ] Here is the [dbt command line cheat sheet](https://about.gitlab.com/handbook/business-ops/data-team/platform/dbt-guide/#command-line-cheat-sheet)
- Note: When launching dbt you will see `WARNING: The GOOGLE_APPLICATION_CREDENTIALS variable is not set. Defaulting to a blank string.` Unless you are developing on Airflow this is ok and expected. If you require GOOGLE_APPLICATION_CREDENTIALS please follow the steps outlined below in the DataLab section.

## DataLab (Jupyter setup)

Data team currently uses DataLab (Jupyter in cloud provided by Google Cloud) to conduct analysis and build models with Python. Follow below steps to get running instance for yourself.

- [ ] Raise Access Request (AR) for Google Cloud Credentials. To do that please follow instructions here or create separate issue and copy contents from [here](https://about.gitlab.com/handbook/business-technology/team-member-enablement/onboarding-access-requests/access-requests/) or create separate issue and copy contents from [here](https://gitlab.com/gitlab-com/team-member-epics/access-requests/-/issues/10306#note_622125437). Ensure you update your name and other personal details and project name is ``gitlab-analysis.`` Assign it to your manager.
- [ ] Raise AR for DataLab setup for ``gitlab-analysis``, similar way as previous step. Assign it to your manager. You can also tag project owners (Dennis van Rooijen, Paul Armstrong or Ved Prakash) if you need help.
- [ ] Please follow next step after running onboarding template, once you added GOOGLE_APPLICATION_CREDENTIALS path to your .zshrc` file which can be accessed by vi ~/.zshrc``. One of the project owners should send you configuration json file, which is important to add in your google credentials. Follow below steps:
- [ ] Download the json file provided and move to your home directory (e.g. `/Users/yourusername`)
- [ ] Open terminal and run the following command, replacing `yourusername` with your actual user name on your computer (type `pwd` into the terminal if you don’t know it — the path should contain your user name) and `filename.json` with you name of the file.
    - echo export  GOOGLE_APPLICATION_CREDENTIALS=/Users/yourusername/filename.json >> ./.zshrc
    - If you already have the variable  GOOGLE_APPLICATION_CREDENTIALS  modify its value to the file path and file name instead of adding a new one. 
- [ ] Refresh this file by sourcing it back, by running command in terminal: ``source ~/.zshrc``.
- [ ] After approved AR install and initialise Google Cloud SDK (which stands for software development kit) to which instructions are provided [here](https://cloud.google.com/sdk/docs/install).. After download and installation follow point a and b, especially commands in terminal.
- [ ] Run ``gcloud components install datalab`` in your terminal
- [ ] Run `gcloud auth login to authenticate` with your gitlab gmail account
- [ ] Project owner should provide you name of your Datalab instance, the most likely it will be your_gitlab_handle-datalab-project. If you do not receive it follow up with owners by tagging them in access request issue or texting them directly on slack. Once you have name of your instance connect to DataLab by using `datalab connect your_gitlab_username-datalab-project`. 

- If you receive error "The specified Datalab instance was created for your_gitlab_username@gitlab.com, but you are attempting to connect to it as your_gitlab_username@gitlab-analysis.iam.gserviceaccount.com". Then re-run the command as `datalab connect your_gitlab_username-datalab-project --no-user-checking`

- [ ] Open your browser and type localhost:8081. It may take couple minutes to connect, so if nothing comes up refresh website or validate with project owners if your access has been granted properly. You should be all set!


## Jupyter 

- [ ] Ensure you've setup your dbt for running locally as mentioned above. The ./.dbt/profiles.yml file is a pre-requisite for this process. If you do not want dbt you can manually create the ./.dbt/profiles.yml file based off the [sample profile](https://gitlab.com/gitlab-data/analytics/-/blob/master/admin/sample_profiles.yml)
- [ ] Clone the DataScience repo: 
    ``` git clone https://gitlab.com/gitlab-data/data-science```
- [ ] Run `make jupyter` from the root directory of the repository and confirm that JupyterLab has now spun up on successfully. 
- [ ] Run through the notebook at `./notebooks/templates/auth_example.ipynb` to confirm that you have configured everything successfully.  

## GitLab.com (Product)
This data comes from our GitLab.com SaaS product.
- [ ] Become familiar with the [API docs](https://gitlab.com/gitlab-org/gitlab/tree/master/doc/api)
- [ ] This is the [schema for the database](https://gitlab.com/gitlab-org/gitlab/-/blob/master/db/structure.sql)
- [ ] If you ever want to know what queries are going on in the background while you're using GitLab.com, enable the [Performance Bar](https://docs.gitlab.com/ee/administration/monitoring/performance/performance_bar.html) and click on the numbers to the left of `pg`. This is useful for learning how the gitlab.com schema works. The performance bar can be enable by pressing `p + b` ([Shortcut Docs](https://docs.gitlab.com/ee/user/shortcuts.html)).

## Marketo
- [ ] [Coming soon]
- [ ] For access to Marketo, your manager will need to create an [Access Request](https://gitlab.com/gitlab-com/access-requests/issues/new?issuable_template=New%20Access%20Request). Please confirm with your manager that this has been done.

## Netsuite (Accounting)
- [ ] Netsuite dbt models 101: Familiarize yourself with the Netsuite models by watching this [Data Netsuite dbt models](https://www.youtube.com/watch?v=u2329sQrWDY&feature=youtu.be). You will need to be logged into [GitLab Unfiltered](https://www.youtube.com/channel/UCMtZ0sc1HHNtGGWZFDRTh5A/).
- [ ] For access to Netsuite, your manager will need to create an [Access Request](https://gitlab.com/gitlab-com/access-requests/issues/new?issuable_template=New%20Access%20Request). Please confirm with your manager that this has been done.



## Misc

- [ ] Familiarize yourself with the [Stitch](http://stitchdata.com) UI, as this is mostly the source of truth for what data we are loading. An email will have been sent with info on how to get logged in.
- [ ] Familiarize yourself with GitLab CI https://docs.gitlab.com/ee/ci/quick_start/ and our running pipelines.
- [ ] Consider joining [Locally Optimistic slack](https://www.locallyoptimistic.com/community/)
 (Not required, but recommended).
- [ ] Consider subscribing to the [Data Science Roundup](http://roundup.fishtownanalytics.com) (Not required, but recommended).
- [ ] There are many Slack channels organized around interests, such as `#fitlab`, `#bookclub`, and `#woodworking`. There are also many organized by location (these all start with `#loc_`). This is a great way to connect to GitLab team members outside of the Data-team. Join some that are relevant to your interests, if you'd like.
- [ ] Familiarize yourself with [SheetLoad](https://about.gitlab.com/handbook/business-ops/data-team/platform/#using-sheetload).
- [ ] Really really useful resources in [this Drive folder](https://drive.google.com/drive/folders/1wrI_7v0HwCwd-o1ryTv5dlh6GW_JyrSQ?usp=sharing) (GitLab Internal); Read the `a_README` file first.
- [ ] Save the [Data Kitchen Data Ops Cookbook](https://drive.google.com/file/d/14KyYdFB-DOeD0y2rNyb2SqjXKygo10lg/view?usp=sharing) as a reference.
- [ ] Save the [Data Engineering Cookbook](https://drive.google.com/file/d/1Tm3GiV3P6c5S3mhfF9bm7VaKCtio-9hm/view?usp=sharing) as a reference.

# Usage/Version Ping (Product)
This data comes from the usage ping that comes with a GitLab installation.
- [ ] Read about the [usage ping](https://docs.gitlab.com/ee/user/admin_area/settings/usage_statistics.html).
- [ ] To understand how this is implemented at GitLab read [Feature Implementation](https://about.gitlab.com/handbook/product/feature-instrumentation/#instrumentation-for-gitlabcom).
- [ ] Read the product vision for [telemetry](https://about.gitlab.com/direction/telemetry/).
- [ ] There is not great documentation on the usage ping, but you can get a sense from looking at the `usage.rb` file for [GitLab CE](https://gitlab.com/gitlab-org/gitlab/blob/master/lib/gitlab/usage_data.rb).
- [ ] It might be helpful to look at issues related to telemetry [here](https://gitlab.com/gitlab-org/telemetry/issues) and [here](https://gitlab.com/groups/gitlab-org/-/issues?scope=all&utf8=✓&state=all&search=~telemetry).
- [ ] Watch the [pings brain dump session](https://drive.google.com/file/d/1S8lNyMdC3oXfCdWhY69Lx-tUVdL9SPFe/view).  This video is outdated.  The tables that are related to the usage ping now reside in the [version model](https://dbt.gitlabdata.com/#!/model/model.gitlab_snowflake.version_usage_data).

## Salesforce (Sales, Marketing, Finance)
Also referred as SFDC, Salesforce.com (Sales Force Dot Com).
- [ ] Become familiar with Salesforce using [Trailhead](https://trailhead.salesforce.com/).
- [ ] If you are new to Salesforce or CRMs in general, start with [Intro to CRM Basics](https://trailhead.salesforce.com/trails/getting_started_crm_basics).
- [ ] If you have not used Salesforce before, take this [intro to the platform](https://trailhead.salesforce.com/trails/force_com_admin_beginner/modules/starting_force_com).
- [ ] To familiarize yourself with the Salesforce data model, take [Data Modeling](https://trailhead.salesforce.com/trails/force_com_admin_beginner/modules/data_modeling).
- [ ] You can review the general data model in [this reference](https://developer.salesforce.com/docs/atlas.en-us.api.meta/api/data_model.htm). Pay particular attention to the [Sales Objects](https://developer.salesforce.com/docs/atlas.en-us.api.meta/api/sforce_api_erd_majors.htm).
- [ ] To familiarize yourself with the Salesforce APIs, take [Intro to SFDC APIs](https://trailhead.salesforce.com/trails/force_com_dev_intermediate/modules/api_basics).
- [ ] For access to SFDC, your manager will need to create an [Access Request](https://gitlab.com/gitlab-com/access-requests/issues/new?issuable_template=New%20Access%20Request). Please confirm with your manager that this has been done.
- [ ] Watch the [SalesForce brain dump session](https://youtu.be/KwG3ylzWWWo).

## Snowplow (Product)
[Snowplow](https://snowplowanalytics.com) is an open source web analytics collector.
- [ ] To understand how this is implemented at GitLab read [Feature Implementation](https://about.gitlab.com/handbook/product/feature-instrumentation/#instrumentation-for-gitlabcom).
- [ ] Also read how we pull data from [S3 into Snowflake](https://about.gitlab.com/handbook/business-ops/data-team/platform/#snowplow-infrastructure)
- [ ] Familiarize yourself with the [Snowplow Open Source documentation](https://github.com/snowplow/snowplow).
- [ ] We use the [Snowplow dbt package](https://hub.getdbt.com/fishtown-analytics/snowplow/latest/) on our models. Their documentation does show up in our dbt docs.

## Zendesk
- [ ] For access to Zendesk, please follow the instructions in the [handbook](https://about.gitlab.com/handbook/support/internal-support/#light-agent-zendesk-accounts-available-for-all-gitlab-staff)

## Zuora (Finance, Billing SSOT)
- [ ] Become familiar with Zuora.
- [ ] Watch Brian explain Zuora to Taylor [GDrive Link](https://drive.google.com/file/d/1fCr48jZbPiW0ViGr-6rZxVVdBpKIoopg/view).
- [ ] [Zuora documentation](https://knowledgecenter.zuora.com/).
- [ ] [Data Model from Zuora for Salesforce](https://knowledgecenter.zuora.com/CA_Commerce/A_Zuora_CPQ/A2_Zuora4Salesforce_Object_Model).
- [ ] [Data Model inside Zuora](https://knowledgecenter.zuora.com/BB_Introducing_Z_Business/D_Zuora_Business_Objects_Relationship).
- [ ] [Definitions of Objects](https://knowledgecenter.zuora.com/CD_Reporting/D_Data_Sources_and_Exports/AB_Data_Source_Availability).
- [ ] [Zuora Subscription Data Management](https://about.gitlab.com/handbook/finance/accounting/#zuora-subscription-data-management).
- [ ] For access to Zuora, your manager will need to create an [Access Request](https://gitlab.com/gitlab-com/access-requests/issues/new?issuable_template=New%20Access%20Request). Please confirm with your manager that this has been done.

### Metrics and Methods
- [ ] Read through [SaaS Metrics 2.0](http://www.forentrepreneurs.com/saas-metrics-2/) to get a good understanding of general SaaS metrics.
- [ ] Check out [10 Reads for Data Scientists Getting Started with Business Models](https://www.conordewey.com/blog/10-reads-for-data-scientists-getting-started-with-business-models/) and read through the collection of articles to deepen your understanding of SaaS metrics.
- [ ] Familiarize yourself with the GitLab Metrics Sheet (search in Google Drive, it should come up) which contains most of the key metrics we use at GitLab and the [definitions of these metrics](https://about.gitlab.com/handbook/business-ops/data-team/kpi-index/).
- [ ] Optional, for more information on Finance KPIs, you can watch this working session between the Manager, Financial Planning and Analysis and Data Analyst, Finance: [Finance KPIs](https://www.youtube.com/watch?v=dmdilBQb9PY&feature=youtu.be)

## Triage
Data triagers are the first responders to requests and problems for the Data team.
- [ ] Read about the Triage  proces in our [handbook](##%20Triage%20%20Data%20triagers%20are%20the%20first%20responders%20to%20requests%20and%20problems%20for%20the%20Data%20team.%20-%20%5B%20%5D) 
- [ ] Checkout the Triage [template](https://gitlab.com/gitlab-data/analytics/-/blob/master/.gitlab/issue_templates/Data%20Triage.md)

## Good First Issues:
- [ ] [Replace]
- [ ] [Replace]

## Resources to help you get started with your first issue
- [ ] Pairing session between a new Data Analyst and a Staff Data Engineer working on the new analyst's first issue: [Pair on Lost MRR Dashboard Creation](https://www.youtube.com/watch?v=WuIcnpuS2Mg)
- [ ] 2nd part of pairing session between a new Data Analyst and a Staff Data Engineer working on the new analyst's first issue: [Pair on Lost MRR Dashboard Creation Part 2](https://www.youtube.com/watch?v=HIlDH5gaL3M)
- [ ] Setting up visual studio and git terminals to use for testing locally. (https://youtu.be/t5eoNLUl3x0)

 (Not required, but recommended).
- [ ] [Company Call Agenda](https://docs.google.com/document/d/1JiLWsTOm0yprPVIW9W-hM4iUsRxkBt_1bpm3VXV4Muc/edit)
- [ ] [DataOps Meeting Agenda](https://docs.google.com/document/d/1qCfpRRKQfSU3VplI45huE266CT0nB82levb3lF9xeUs/edit)
- [ ] Optional, for more information on Finance KPIs, you can watch this working session between the Manager, Financial Planning and Analysis and Data Analyst, Finance: [Finance KPIs](https://www.youtube.com/watch?v=dmdilBQb9PY&feature=youtu.be)
- [ ] Watch @tlapiana's [talk at DataEngConf](https://www.youtube.com/watch?v=eu623QBwakc) that gives a phenomenal overview of how the team works.
- [ ] Watch [this great talk](https://www.youtube.com/watch?v=prcz0ubTAAg) on what Analytics is
- [ ] If relevant, watch ["The State of [Product] Data"](https://www.youtube.com/watch?v=eNLkj3Ho2bk&feature=youtu.be) from Eli at the Growth Fastboot. (You'll need to be logged into GitLab Unfiltered.)

## Suggested Bookmarks None of these are required, but bookmarking these links will make life at GitLab much easier. Some of these are not hyperlinked for security concerns.
- [ ] 1:1 with Manager Agenda
- [ ] [Create new issue in Analytics Project](https://gitlab.com/gitlab-data/analytics/issues/new?issue%5Bassignee_id%5D=&issue%5Bmilestone_id%5D=)
- [ ] [Data team page of Handbook](https://about.gitlab.com/handbook/business-ops/data-team/)
- [ ] [dbt Docs](https://docs.getdbt.com)
- [ ] [dbt Discourse](http://discourse.getdbt.com)
- [ ] [GitLab's dbt Documentation](https://dbt.gitlabdata.com)
- [ ] [Data Team GitLab Activity](https://gitlab.com/groups/gitlab-data/-/activity)
        


