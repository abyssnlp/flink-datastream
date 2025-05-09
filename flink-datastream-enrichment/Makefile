VERSION = "1.0"
DOCKER_USER = ${docker_username}


ensure-kubectl-helm:
	@echo "Checking if kubectl and helm are installed..."
	@command -v kubectl >/dev/null 2>&1 || { echo >&2 "kubectl is not installed. Aborting."; exit 1; }
	@command -v helm >/dev/null 2>&1 || { echo >&2 "helm is not installed. Aborting."; exit 1; }

deploy-producer-job: ensure-kubectl-helm
	@echo "Deploying orders producer job"
	@if [ -z "$(DOCKER_USER)" ]; then \
    		echo "Error: Docker registry username not set. Set docker_username variable." ;\
    		echo "ex. make deploy-producer-job docker_username=abyssnlp" >&2 ;\
    		exit 1 ;\
	fi
	@echo "Docker user: $(DOCKER_USER)"
	cd scripts/producer && docker buildx build -f Dockerfile -t $(DOCKER_USER)/orders-producer:0.1 . && \
    kubectl -n kafka apply -f ../../k8s/producer-job.yml

deploy-lookup-job: ensure-kubectl-helm
	@echo "Deploying lookup job"
	@if [ -z "$(DOCKER_USER)" ]; then \
    		echo "Error: Docker registry username not set. Set docker_username variable." ;\
    		echo "ex. make deploy-lookup-job docker_username=abyssnlp" >&2 ;\
    		exit 1 ;\
	fi
	@echo "Docker user: $(DOCKER_USER)"
	cd scripts/lookup && docker buildx build -f Dockerfile -t $(DOCKER_USER)/customers-lookup:0.1 . && \
	kubectl apply -f k8s/lookup-table-job.yml

setup-topics: ensure-kubectl-helm
	@echo "Setting up Kafka topics"
	kubectl -n kafka apply -f k8s/topic.yml && \
	kubectl -n kafka apply -f k8s/enriched-topic.yml

setup-secrets: ensure-kubectl-helm
	@echo "Setting up secrets for running the flink job"
	kubectl apply -f k8s/secrets.yml

run-flink-enrichment:
	@echo "Running Flink enrichment job; make sure to run setup-secrets first"
	@if [ -z "$(DOCKER_USER)" ]; then \
    		echo "Error: Docker registry username not set. Set docker_username variable." ;\
    		echo "ex. make run-flink-enrichment docker_username=abyssnlp" >&2 ;\
    		exit 1 ;\
	fi
	@echo "Docker user: $(DOCKER_USER)"
	docker buildx build -f Dockerfile -t $(DOCKER_USER)//flink-datastream-enrichment:1.0 . && \
	kubectl apply -f k8s/deploy.yml
