# Makefile for local development


# Retrieves present working directory (./abaco) and sets :dev tag
export abaco_path = ${PWD}
export TAG = :dev

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

# Starts up local environment (docker containers) in daemon mode
deploy: build
	@docker-compose up -d

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

# Cleanly remove docker images necessary for local-dev
clean:
	@docker-compose down

# Removes/ends all active Docker containers
force-clean:
	@docker rm -f `docker ps -aq`

# WARNING: Deletes all non-active Docker images
prune:
	@docker system prune -a