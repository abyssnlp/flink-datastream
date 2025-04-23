Flink datastream enrichment
===

This is the accompanying repository for the blog
post [Flink datastream enrichment](https://www.unskewdata.com/blog/stream-flink-4).
It contains a simple example of how to use the Flink datastream API to enrich a stream of data with a lookup table. In
this example,
we will use a Kafka source and sink and a Postgres look up table. The example is deployed on Kubernetes using the Flink
operator.

## Prerequisites

- Docker
- Kubernetes
- kubectl
- Kafka
- Postgres
- Flink operator

For setting up Kafka, Flink and Postgres on Kubernetes, you can refer to the following resources:

- [Stream Processing with Apache Flink. Part -3](https://www.unskewdata.com/blog/stream-flink-3)
- [Flink CDC Iceberg Repository](https://github.com/abyssnlp/flink-cdc-iceberg) It contains a makefile and manifests to
  deploy Kafka, Postgres and Flink on Kubernetes.

## Running the example

```shell
# substitute with your own docker username
make run-flink-enrichment docker_username=abyssnlp
```
