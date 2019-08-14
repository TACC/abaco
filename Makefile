# Makefile for local development

.PHONY: clean prune nuke

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


# Builds Docker images to run local-dev (abaco/core, abaco/prom, abaco/nginx, etc.)
build:
	### Local Dev Images ###
	@docker pull abaco/core$$TAG
	@docker pull abaco/nginx$$TAG

	### Database images ###
	@docker pull rabbitmq:3.5.3-management
	@docker pull redis
	@docker pull mongo

	### In-Development Images ###
	@docker pull abaco/prom$$TAG
	@docker pull abaco/dashboard


# Build local
local-build:
	@docker build ./ -t abaco/core:dev


# Starts up local environment (docker containers) in daemon mode
deploy: build
	@docker-compose up -d


local-deploy: local-build
	@docker-compose up -d

# Deploys, but not silently and will abort if a container exits.
test-deploy:
	@docker-compose up --abort-on-container-exit


# Runs test suite against current repository
test: deploy
	@./build_and_test.sh


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


# Test variables
test-vars:
	@echo $$TAG
	@echo $$abaco_path


# Ends all active Docker containers needed for abaco
clean:
	@docker-compose down


# Does a clean and also deletes all images needed for abaco
prune:
	@docker-compose down --rmi all


# Delete all images forcefully (so it also deletes containers)
nuke:
	@docker rm -f `docker ps -aq`
