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
export TAG := :dev
endif


# Gets all images and starts Abaco suite in daemon mode
deploy:	
	@docker rmi abaco/core$$TAG
	@docker pull abaco/core$$TAG
	@docker-compose up -d


# Builds core locally and sets to correct tag. This should take priority over DockerHub images
build:
	@docker build ./ -t abaco/core$$TAG


# Builds core locally and then runs with Abaco suite with that abaco/core image in daemon mode
local-deploy: build
	@docker-compose up -d


# Deploys, but not silently and will abort if a container exits.
test-deploy:
	@docker-compose up --abort-on-container-exit


# Pulls all Docker images not yet available but needed to run Abaco suite
pull:
	@docker-compose pull


# Runs test suite against current repository
# Runs specific test based on the 'test' environment variable
# ex: export test=test/load_tests.py
test:
	sleep 5
	docker build -t abaco/testsuite$$TAG -f Dockerfile-test .
	docker run --network=abaco_abaco -e base_url=http://nginx -e maxErrors=1 -e case=camel -v /:/host -v $$abaco_path/local-dev.conf:/etc/service.conf -it --rm abaco/testsuite$$TAG $$test


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
