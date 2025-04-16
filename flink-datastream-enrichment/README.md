Flink datastream enrichment
===

```shell
kubectl apply -f k8s/secrets.yml
kubectl -n kafka apply -f k8s/topic.yml 

# build producer and deploy to k8s
cd scripts/producer && docker buildx build -f Dockerfile -t abyssnlp/orders-producer:0.1 .
kubectl -n kafka apply -f k8s/producer-job.yml

# build lookup table job and deploy to k8s
cd scripts/lookup && docker buildx build -f Dockerfile -t abyssnlp/customers-lookup:0.1 .
kubectl apply -f k8s/lookup-table-job.yml

# execute flink job
docker buildx build -f Dockerfile -t abyssnlp/flink-datastream-enrichment:1.0 .
kubectl apply -f k8s/deploy.yml
```
