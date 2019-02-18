# Makefile for local development


# Retrieves present working directory (./abaco) and sets :dev tag
export abaco_path = ${PWD}
export TAG = :dev

# Echos path and tag for debug purposes. Starts up local environment(docker containers) in daemon mode
local-dev:
	@echo abaco_path=$$abaco_path
	@echo TAG=$$TAG
	@docker-compose up -d

# Builds a few sample Docker images
samples:
	@docker build -t abaco_test -f samples/abaco_test/Dockerfile samples/abaco_test
	@docker build -t docker_ps -f samples/docker_ps/Dockerfile samples/docker_ps
	@docker build -t word_count -f samples/word_count/Dockerfile samples/word_count
	@echo wat

# Creates every Docker image in samples folder. NEED: a way to remove README.md from list
dirs := samples
files_read := $(foreach dir,$(dirs),$(wildcard $(dir)/*))
#files := $(filter-out samples/README.md samples/agave_py3,$(files&read))

all-samples:
	@for file in $(files_read); do \
		docker build -t $$file -f $$file/Dockerfile $$file; \
	done


# Removes/ends all active Docker containers
clean:
	@docker rm -f `docker ps -aq`

# WARNING: Deletes all non-active Docker images
prune:
	@docker system prune -a

# Runs test suite against current repository
test:
	@./build_and_test.sh