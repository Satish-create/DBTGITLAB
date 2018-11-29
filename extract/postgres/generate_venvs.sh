#!/bin/bash
apt-get -y install python3-venv

mkdir -p venvs

python3 -m venv venvs/tap-postgres
venvs/tap-postgres/bin/pip3 install --upgrade pip
venvs/tap-postgres/bin/pip3 install --upgrade setuptools
venvs/tap-postgres/bin/pip3 install tap-postgres

python3 -m venv venvs/target-snowflake
venvs/target-snowflake/bin/pip3 install --upgrade pip
venvs/target-snowflake/bin/pip3 install --upgrade setuptools
venvs/target-snowflake/bin/pip3 install git+https://gitlab.com/meltano/target-snowflake.git