.PHONY: build

IMAGE := analytics_webserver_1

help:
	@echo "\n \
	**List of Makefile commands** \n \
	attach: attaches a shell to airflow deployment in docker-compose.yml. \n \
	cleanup: WARNING: DELETES DB VOLUME, frees up space and gets rid of old containers/images. \n \
	compose: spins up an airflow deployment in the background and mounts the analytics repo. \n \
	init: initializes a new Airflow db, required on a fresh db. \n"

attach: compose
	@echo "Attaching to the Webserver container..."
	@docker exec -ti ${IMAGE} /bin/bash

cleanup:
	@echo "Cleaning things up..."
	@docker-compose down -v
	@docker system prune -f

compose:
	@echo "Composing airflow..."
	@docker-compose up -d

init:
	@echo "Initializing the Airflow DB..."
	@docker-compose up -d db
	@sleep 10
	@docker-compose run scheduler airflow initdb
	@docker-compose down

set-branch:
	@echo "Run this command to properly set your GIT_BRANCH env var:"
	@echo "export GIT_BRANCH=$$(git symbolic-ref --short HEAD)"
