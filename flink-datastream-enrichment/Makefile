VERSION = "1.0"
DOCKER_USER = ${docker_username}


ensure-kubectl-helm:
	@echo "Checking if kubectl and helm are installed..."
	@command -v kubectl >/dev/null 2>&1 || { echo >&2 "kubectl is not installed. Aborting."; exit 1; }
	@command -v helm >/dev/null 2>&1 || { echo >&2 "helm is not installed. Aborting."; exit 1; }

deploy-producer-job: ensure-kubectl-helm setup-topics-secrets
	@echo "Deploying orders producer job"
	cd scripts/producer && docker buildx build -f Dockerfile -t $(DOCKER_USER)/orders-producer:0.1 . && \
    kubectl -n kafka apply -f ../../k8s/producer-job.yml

 deploy-lookup-job: ensure-kubectl-helm setup-topics-secrets
 	@echo "Deployting lookup job"
 	cd scripts/lookup && docker buildx build -f Dockerfile -t $(DOCKER_USER)//customers-lookup:0.1 . && \
    kubectl apply -f k8s/lookup-table-job.yml

setup-topics-secrets: ensure-kubectl-helm
	@echo "Setting up Kafka topics and secrets"
	kubectl apply -f k8s/secrets.yml && \
    kubectl -n kafka apply -f k8s/topic.yml && \
    kubectl -n kafka apply -f k8s/enriched-topic.yml

run-flink-enrichment: deploy-producer-job deploy-lookup-job
	@echo "Running Flink enrichment job"
	docker buildx build -f Dockerfile -t $(DOCKER_USER)//flink-datastream-enrichment:1.0 . && \
    kubectl apply -f k8s/deploy.yml
