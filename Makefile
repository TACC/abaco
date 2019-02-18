# Makefile to start up local development environment


# Retrieves present working directory (./abaco) and sets :dev tag
export abaco_path = ${PWD}
export TAG = :dev


# Echos path and tag for debug purposes. Starts up docker container in daemon mode
local-dev:
	@echo $$abaco_path
	@echo $$TAG
	@docker-compose up -d

# Builds docker image for generic 'Dockerfile-actor'
build: Dockerfile-actor
	@docker build -t myactor -f Dockerfile-actor . 

# Removes/ends all active docker containers
clean:
	@docker rm -f `docker ps -aq`

# Runs tests against current repository
#test: