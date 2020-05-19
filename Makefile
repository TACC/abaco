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
	@docker rmi abaco/core:$$TAG
	@docker pull abaco/core:$$TAG
	@docker-compose up -d


# Builds core locally and sets to correct tag. This should take priority over DockerHub images
build-core:
	@docker build -t abaco/core:$$TAG ./


# Builds nginx
build-nginx:
	@docker build -t abaco/nginx:$$TAG images/nginx/.


# Builds testsuite
build-testsuite:
	@docker build -t abaco/testsuite:$$TAG -f Dockerfile-test .


# Builds core locally and then runs with Abaco suite with that abaco/core image in daemon mode
local-deploy: build-core build-nginx
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
	sed -i.bak 's/case: camel/case: snake/g' local-dev.conf


# Builds local everything and performs testsuite for camel case.
# Can run specific test based on the 'test' environment variable
# ex: export test=test/load_tests.py
test-camel: build-testsuite
	@echo "\n\nCamel Case Tests.\n"
	@echo "Converting config file to camel case and launching Abaco Stack.\n"
	sed -i.bak 's/case: snake/case: camel/g' local-dev.conf; make local-deploy; sleep $$docker_ready_wait; docker run $$interactive --network=abaco_abaco -e base_url=http://nginx -e maxErrors=$$maxErrors -e case=camel -v /:/host -v $$abaco_path/local-dev.conf:/etc/service.conf --rm abaco/testsuite:$$TAG $$test

# Builds local everything and performs testsuite for snake case.
# Converts local-dev.conf back to camel case after test.
# Can run specific test based on the 'test' environment variable
# ex: export test=test/load_tests.py
test-snake: build-testsuite
	@echo "\n\nSnake Case Tests.\n"
	@echo "Converting config file to snake case and launching Abaco Stack.\n"
	sed -i.bak 's/case: camel/case: snake/g' local-dev.conf; make local-deploy; sleep $$docker_ready_wait; docker run $$interactive --network=abaco_abaco -e base_url=http://nginx -e maxErrors=$$maxErrors -e case=snake -v /:/host -v $$abaco_path/local-dev.conf:/etc/service.conf --rm abaco/testsuite:$$TAG $$test
	@echo "Converting back to camel"; sed -i.bak 's/case: snake/case: camel/g' local-dev.conf


# Pulls all Docker images not yet available but needed to run Abaco suite
pull:
	@docker-compose pull


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


# Test setting of environment variables
vars:
	@echo $$TAG
	@echo $$abaco_path
	@echo $$interactive


# Ends all active Docker containers needed for abaco
down:
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
