# Makefile for local development

# Colors in echos: https://stackoverflow.com/questions/5947742/how-to-change-the-output-color-of-echo-in-linux
# Colors
BLACK=\033[0;30m
RED=\033[0;31m
GREEN=\033[0;32m
ORANGE=\033[0;33m
BLUE=\033[0;34m
PURPLE=\033[0;35m
CYAN=\033[0;36m
GRAY=\033[1;30m

# Light colors
WHITE=\033[1;37m
LRED=\033[1;31m
LGREEN=\033[1;32m
YELLOW=\033[1;33m
LBLUE=\033[1;34m
LPURPLE=\033[1;35m
LCYAN=\033[1;36m
LGRAY=\033[0;37m

# No color
NC=\033[0m

.ONESHELL: down
.PHONY: down clean nuke help

# TAG to use for service image
# options: "dev" | "whatever"
# default: "dev"
export TAG := dev

# BACKEND to use containers. Either minikube backend or regular local backend
# options: "minikube" | "docker"
# default: "minikube"
export BACKEND := docker

# IMG_SOURCE to get images from, either locally built or remotely pulled
# options: "local" | "remote"
# default: "local"
export IMG_SOURCE := local

# NAMESPACE for minikube instance to use.
# options: "default" | "whatever"
# default: "default"
export NAMESPACE := default

# SERVICE_NAME to use throughout. Changes deployment folder. Have to modify here too.
# options: "whatever"
# default: "actors"
export SERVICE_NAME := abaco

# SERVICE_PASS to use throughout. Must be filled.
export SERVICE_PASS := ***REMOVED***

# DEV_TOOLS bool. Whether or not to start jupyter + mount kg_service folder in pods (main).
# options: "false" | "true"
# default: "false"
export DEV_TOOLS := true


ifdef abaco_path
export abaco_path := $(abaco_path)
else
export abaco_path := $(PWD)
endif

ifdef in_jenkins
unexport interactive
else
export interactive := -it
endif

ifdef api_ready_wait_time
export api_ready_wait_time := $(api_ready_wait_time)
else
export api_ready_wait_time := 10
endif

ifdef maxErrors
export maxErrors := $(maxErrors)
else
export maxErrors := 999
endif



# Got from: https://stackoverflow.com/a/59087509
help:
	@grep -B1 -E "^[a-zA-Z0-9_-]+\:([^\=]|$$)" Makefile \
	| grep -v -- -- \
	| sed 'N;s/\n/###/' \
	| sed -n 's/^#: \(.*\)###\(.*\):.*/\2###\1/p' \
	| column -t  -s '###'


# Gets all remote images and starts abaco in backend mode
#: Deploy service
up: vars build
	@echo "Makefile: $(GREEN)up$(NC)"

ifeq ($(BACKEND),docker)
	@echo "  🌎 : Using backend: $(LCYAN)docker$(NC)"
	@echo "  🔨 : Changing some parts of config-local.json."
	@sed -i 's/"version".*/"version": "$(TAG)",/g' config-local.json
	@echo "  🔥 : Running docker-compose up."
	@echo ""
	@docker-compose --project-name=abaco up -d

else ifeq ($(BACKEND),minikube)
	@echo "  🌎 : Using backend: $(LCYAN)minikube$(NC)"
	@echo "  🔍 : Looking to run ./burnup in deployment folder."
	rm -rf kube-deployment; mkdir kube-deployment; cp -r kube-template/* kube-deployment;
	cd kube-deployment
	@echo "  🔨 : Created kube-deployment folder with templates."
	@sed -i 's/"version".*/"version": "$(TAG)",/g' config.json
	@sed -i 's/MAKEFILE_NAMESPACE/$(NAMESPACE)/g' *
	@sed -i 's/MAKEFILE_SERVICE_NAME/$(SERVICE_NAME)/g' *
	@sed -i 's/MAKEFILE_SERVICE_PASS/$(SERVICE_PASS)/g' *
	@sed -i 's/MAKEFILE_TAG/$(TAG)/g' *
	@echo "  🔥 : Running burnup."
# ifeq ($(DEV_TOOLS),true)
# 	@sed -i 's/#DEV//g' *
# 	echo "  🔗 : Jupyter Lab URL: $(LCYAN)http://$$(minikube ip):$$(kubectl get service $(SERVICE_NAME)-main-jupyter | grep -o -P '(?<=8888:).*(?=/TCP)')$(NC)"
# endif
	@echo ""
	./burnup

endif
	echo ""


# Builds core locally and sets to correct tag. This should take priority over DockerHub images
#: Build core image
build: vars
	@echo "Makefile: $(GREEN)build$(NC)"
	@echo "  🔨 : Running image build for core-v3, prometheus, and nginx."

ifeq ($(BACKEND),minikube)
	$(MAKE) build-minikube
else ifeq ($(BACKEND),docker)
	$(MAKE) build-docker
endif


build-docker: vars
	@echo "Makefile: $(GREEN)build$(NC)"
	@echo "  🔨 : Running image build for core-v3, prometheus, and nginx."

	@echo "  🌎 : Using backend: $(LCYAN)docker$(NC)"
	@echo ""
	docker build -t $(SERVICE_NAME)/core-v3:$$TAG ./
	@echo ""
	docker build -t $(SERVICE_NAME)/prom:$$TAG images/prometheus/.
	@echo ""
	docker build -t $(SERVICE_NAME)/nginx:$$TAG images/nginx/.
	@echo ""


build-minikube: vars
	@echo "Makefile: $(GREEN)build$(NC)"
	@echo "  🔨 : Running image build for core-v3, prometheus, and nginx."

	@echo "  🌎 : Using backend: $(LCYAN)minikube$(NC)"
	@echo ""
	minikube image build -t $(SERVICE_NAME)/core-v3:$$TAG ./
	minikube image build -t $(SERVICE_NAME)/prom:$$TAG images/prometheus/.
	minikube image build -t $(SERVICE_NAME)/nginx:$$TAG images/nginx/.
	@echo ""


#: Pull core image
pull:
	@echo "Makefile: $(GREEN)pull$(NC)"
	@echo "Not yet implemented"

# Ends all active k8 containers needed for kgservice
# This kills any workers on the "kgservice_kgservice" network.
# In order to work on any network, the script needs to someone get network from the docker-compose.yml
#: Delete service
down:
	@echo "Makefile: $(GREEN)down$(NC)"

ifeq ($(BACKEND),docker)
	@echo "  🌎 : Using backend: $(LCYAN)docker$(NC)"
	@echo "  🔥 : Deleting all containers with abaco_abaco network + docker-compose down."
	@echo ""
	@docker kill `docker network inspect abaco_abaco --format '{{range $$k, $$v := .Containers}}{{printf "%s\n" $$k}}{{end}}'` 2>/dev/null || true
	@docker-compose down
else ifeq ($(BACKEND),minikube)
	@echo "  🌎 : Using backend: $(LCYAN)minikube$(NC)"
	@echo "  🔍 : Looking to run ./burndown in deployment folder."
	if [ -d "kube-deployment" ]; then
		echo "  🎉 : Found kube-deployment folder. Using burndown."
		cd kube-deployment
		echo "  🔥 : Running burndown."
		echo ""
		./burndown
	else
		echo "  ✔️  : No kube-deployment folder, nothing to burndown."
	fi
endif


# Builds local everything and runs both camel case and snake case tests.
# Converts local-dev.conf back to snake case after test.
# Can run specific test based on the 'test' environment variable
# ex: export test=test/load_tests.py
#: Run tests, expects Abaco to already be running.
test: build-docker
	@echo "Makefile: $(GREEN)test-snake$(NC)"

ifeq ($(BACKEND),docker)
	$(MAKE) test-docker
else ifeq ($(BACKEND),minikube)
	$(MAKE) test-minikube
endif

test-docker:
	@echo "  🌎 : Using backend: $(LCYAN)docker$(NC)"
	@echo "  🐍 : Ensuring snake-case in config-local.json"
	@sed -i 's/"web_case".*/"web_case": "snake",/g' config-local.json	
	@echo "  ⏳ : Waiting $(api_ready_wait_time) seconds to ensure Abaco is ready first"
	sleep $$api_ready_wait_time
	@echo "  📝 : Starting Snake Case Tests"
	@echo ""

	docker run \
	-e TESTS=/home/tapis/tests \
	-e case=snake \
	-e maxErrors=$$maxErrors \
	-e base_url=http://nginx \
	-e _called_from_within_test=True \
	-v /:/host \
	-v $$abaco_path/config-local.json:/home/tapis/config.json \
	-v $$abaco_path/abaco.log:/home/tapis/runtime_files/logs/service.log \
	--entrypoint=/home/tapis/tests/entry.sh \
	--rm \
	$$interactive \
	--network=abaco_abaco \
	abaco/core-v3:$$TAG
#--add-host=host.docker.internal:host-gateway \
#	--network=abaco_abaco \

#	--net=host \

test-minikube:
	@echo "  🌎 : Using backend: $(LCYAN)minikube$(NC)"
	@echo "  🐍 : Ensuring snake-case in config-local.json"
	@sed -i 's/"web_case".*/"web_case": "snake",/g' config-local.json	
	@echo "  ⏳ : Waiting $(api_ready_wait_time) seconds to ensure Abaco is ready first"
	@echo "  📝 : Starting Snake Case Tests"
	@echo ""

	docker run \
	-e TESTS=/home/tapis/tests \
	-e case=snake \
	-e maxErrors=$$maxErrors \
	-e base_url=http://192.168.49.2:30570/v3 \
	-e _called_from_within_test=True \
	-v /:/host \
	-v $$abaco_path/config-local.json:/home/tapis/config.json \
	-v $$abaco_path/abaco.log:/home/tapis/runtime_files/logs/service.log \
	--entrypoint=/home/tapis/tests/entry.sh \
	--rm \
	--net=host \
	$$interactive \
	abaco/core-v3:$$TAG


test-remote:
	docker run \
	$$interactive \
	-e TESTS=/home/tapis/tests \
	-e base_url=http://master.staging.tapis.io/v3/ \
	-e maxErrors=$$maxErrors \
	-e _called_from_within_test=True \
	-e case=camel \
	-e abaco_host_path=$$abaco_path \
	--net=host \
	-v /:/host \
	-v $$abaco_path/config-local.json:/home/tapis/config.json \
	--rm \
	abaco/testsuite:$$TAG




test-camel:
	sed -i 's/"web_case".*/"web_case": "camel",/g' config-local.json
	make local-deploy
	sleep $$api_ready_wait_time
	docker run -e TESTS=/home/tapis/tests -v $$abaco_path/abaco.log:/home/tapis/runtime_files/logs/service.log -e case=camel $$interactive -e maxErrors=$$maxErrors --entrypoint=/home/tapis/tests/entry.sh --network=abaco_abaco -e base_url=http://nginx -e _called_from_within_test=True -v /:/host -v $$abaco_path/config-local.json:/home/tapis/config.json --rm abaco/core-v3:$$TAG
	@echo "Converting back to snake"
	sed -i 's/"web_case".*/"web_case": "snake",/g' config-local.json

# Cleans directory. Notably deletes the kube-deployment folder if it exists
#: Delete service + folders
clean: down
	@echo "Makefile: $(GREEN)clean$(NC)"

ifeq ($(BACKEND),docker)
	@echo "  🌎 : Using backend: $(LCYAN)docker$(NC)"
	@echo "  🧹 : docker-compose down - rmi, deleting orphans, deleting volumes."
	@echo ""
	@docker-compose down --remove-orphans -v

else ifeq ($(BACKEND),minikube)
	@echo "  🌎 : Using backend: $(LCYAN)minikube$(NC)"
	@echo "  🔍 : Looking to delete kube-deployment folder."
	if [ -d "kube-deployment" ]; then
		rm -rf kube-deployment
		echo "  🧹 : kube-deployment folder deleted."
	else
		echo "  ✔️  : kube-deployment folder already deleted."
	fi

endif
	@echo ""


# Test setting of environment variables
#: Lists vars
vars:
	@echo "Makefile: $(GREEN)vars$(NC)"	

	echo "  ℹ️  tag:            $(LCYAN)$(TAG)$(NC)"
	echo "  ℹ️  namespace:      $(LCYAN)$(NAMESPACE)$(NC)"
	echo "  ℹ️  service_name:   $(LCYAN)$(SERVICE_NAME)$(NC)"
	echo "  ℹ️  service_pass:   $(LCYAN)$(SERVICE_PASS)$(NC)"
	echo "  ℹ️  interactive:    $(LCYAN)$(interactive)$(NC)"
	echo "  ℹ️  abaco_path:     $(LCYAN)$(abaco_path)$(NC)"

ifeq ($(filter $(BACKEND),minikube docker),)
	echo "  ❌ backend:         $(RED)BACKEND must be one of ['minikube', 'docker']$(NC)"
	exit 1
else
	echo "  ℹ️  backend:         $(LCYAN)$(BACKEND)$(NC)"
endif

ifeq ($(filter $(IMG_SOURCE),local remote),)
	echo "  ❌ img_source:         $(RED)IMG_SOURCE must be one of ['local', 'remote']$(NC)"
	exit 1
else
	echo "  ℹ️  img_source:     $(LCYAN)$(IMG_SOURCE)$(NC)"
endif

ifeq ($(filter $(DEV_TOOLS),true false),)
	echo "  ❌ dev_tools:      $(RED)DEV_TOOLS must be one of ['true', 'false']$(NC)"
	exit 1
else
	echo "  ℹ️  dev_tools:      $(LCYAN)$(DEV_TOOLS)$(NC)"
endif

	echo ""


# Deletes ALL images, containers, and volumes forcefully
nuke:
	@docker rm -f `docker ps -aq`
	@docker rmi -f `docker images -aq`
	@docker container prune -f
	@docker volume prune -f



### Archived targets.
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

# Builds testsuite
build-testsuite:
	@echo "build-testsuite deprecated; tests now packaged in the abaco/core image."
