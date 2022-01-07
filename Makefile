# Makefile for local development

.PHONY: down clean nuke

# Retrieves present working directory (./abaco) and sets abaco tag based on
# current or default values
ifdef abaco_path
export abaco_path := $(abaco_path)
else
export abaco_path := $(PWD)
endif

ifdef TAG
export TAG := $(TAG)
else
export TAG := dev
endif

ifdef in_jenkins
unexport interactive
else
export interactive := -it
endif

ifdef docker_ready_wait
export docker_ready_wait := $(docker_ready_wait)
else
export docker_ready_wait := 15
endif

ifdef maxErrors
export maxErrors := $(maxErrors)
else
export maxErrors := 999
endif

# Gets all remote images and starts Abaco suite in daemon mode
deploy:	
	@docker rmi abaco/core-v3:$$TAG
	@docker pull abaco/core-v3:$$TAG
	@docker-compose --project-name=abaco up -d


# Builds core locally and sets to correct tag. This should take priority over DockerHub images
build-core:
	@docker build -t abaco/core-v3:$$TAG ./


# Builds prometheus locally
build-prom:
	@docker build -t abaco/prom:$$TAG prometheus/.


# Builds nginx
build-nginx:
	@docker build -t abaco/nginx:$$TAG images/nginx/.


# Builds core locally and then runs with Abaco suite with that abaco/core-v3 image in daemon mode
local-deploy: build-core build-nginx
	sed -i 's/"version".*/"version": "$(TAG)",/g' config-local.json
	@docker-compose --project-name=abaco up -d


# Builds local everything and runs both camel case
# and snake case tests.
# Can run specific test based on the 'test' environment variable
# ex: export test=test/load_tests.py
test:
	@echo "\n\nRunning Both Tests.\n"
	make test-camel
	make down
	make test-snake
	make down
	@echo "Converting back to camel"
	sed -i '' 's/"web_case".*/"web_case": "camel",/g' config-local.json


# Builds local everything and performs testsuite for camel case.
# Can run specific test based on the 'test' environment variable
# ex: export test=test/load_tests.py
test-camel:
	@echo "\n\nCamel Case Tests.\n"
	@echo "Converting config file to camel case and launching Abaco Stack."
	sed -i 's/"version".*/"version": "$(TAG)",/g' config-local.json
	sed -i 's/"web_case".*/"web_case": "camel",/g' config-local.json
	make local-deploy
	sleep $$docker_ready_wait
	docker run -e TESTS=/home/tapis/tests -v $$abaco_path/abaco.log:/home/tapis/runtime_files/logs/service.log -e case=camel $$interactive -e maxErrors=$$maxErrors --entrypoint=/home/tapis/tests/entry.sh --network=abaco_abaco -e base_url=http://nginx -e _called_from_within_test=True -v /:/host -v $$abaco_path/config-local.json:/home/tapis/config.json --rm abaco/core-v3:$$TAG

# Builds local everything and performs testsuite for snake case.
# Converts local-dev.conf back to camel case after test.
# Can run specific test based on the 'test' environment variable
# ex: export test=test/load_tests.py
test-snake:
	@echo "\n\nSnake Case Tests.\n"
	@echo "Converting config file to snake case and launching Abaco Stack."
	sed -i 's/"version".*/"version": "$(TAG)",/g' config-local.json
	sed -i 's/"web_case".*/"web_case": "camel",/g' config-local.json
	make local-deploy
	sleep $$docker_ready_wait
	docker run -e TESTS=/home/tapis/tests -v $$abaco_path/abaco.log:/home/tapis/runtime_files/logs/service.log -e case=snake $$interactive -e maxErrors=$$maxErrors --entrypoint=/home/tapis/tests/entry.sh --network=abaco_abaco -e base_url=http://nginx -e _called_from_within_test=True -v /:/host -v $$abaco_path/config-local.json:/home/tapis/config.json --rm abaco/core-v3:$$TAG
	@echo "Converting back to camel"
	sed -i 's/"web_case".*/"web_case": "camel",/g' config-local.json

test-remote:
	docker run $$interactive -e TESTS=/home/tapis/tests -e base_url=http://master.staging.tapis.io/v3/ -e maxErrors=$$maxErrors -e _called_from_within_test=True  -e case=camel -e abaco_host_path=$$abaco_path -v /:/host $$abaco_path/config-local.json:/home/tapis/config.json --rm abaco/testsuite:$$TAG


# Pulls all Docker images not yet available but needed to run Abaco suite
pull:
	@docker-compose pull

# Builds testsuite
build-testsuite:
	@echo "build-testsuite deprecated; tests now packaged in the abaco/core image."

# Builds a few sample Docker images
samples:
	@docker build -t abaco_test -f samples/abaco_test/Dockerfile samples/abaco_test
	@docker build -t docker_ps -f samples/docker_ps/Dockerfile samples/docker_ps
	@docker build -t word_count -f samples/word_count/Dockerfile samples/word_count


# Creates every Docker image in samples folder
all-samples:
	@for file in samples/*; do \
		if [[ "$$file" != "samples/README.md" ]]; then \
			docker build -t $$file -f $$file/Dockerfile $$file; \
		fi \
	done


# These are recreations of what docker-compose-local.yml, docker-compose-local-db.yml,
# and docker-compose-prom.yml used to do. But without the extra files.
# docker-compose-local.yml
old-compute:
	docker-compose up -d nginx reg mes admin spawner metrics health


# docker-compose-local-db.yml
old-db:
	docker-compose up -d rabbit mongo


# docker-compose-prom.yml
old-prometheus:
	docker-compose up -d prometheus grafana 


# Test setting of environment variables
vars:
	@echo $$TAG
	@echo $$abaco_path
	@echo $$interactive


# Ends all active Docker containers needed for abaco
# This kills any workers on the "abaco_abaco" network.
# In order to work on any network, the script needs to someone get network from the docker-compose.yml
down:
	@docker kill `docker network inspect abaco_abaco --format '{{range $$k, $$v := .Containers}}{{printf "%s\n" $$k}}{{end}}'` 2>/dev/null || true
	@docker-compose down

# Does a clean and also deletes all images needed for abaco
clean:
	@docker-compose down --remove-orphans -v --rmi all 


# Deletes ALL images, containers, and volumes forcefully
nuke:
	@docker rm -f `docker ps -aq`
	@docker rmi -f `docker images -aq`
	@docker container prune -f
	@docker volume prune -f
