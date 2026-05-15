.PHONY: run-job
run-job:
	@echo "Running the job..."
	@docker compose cp target/real-time-f1-1.0.jar jobmanager:/tmp/real-time-f1.jar
	@docker compose exec \
      -e KAFKA_BOOTSTRAP_SERVERS=redpanda:9092 \
      -e KAFKA_GROUP_ID=real-time-f1 \
      -e KAFKA_DRIVERS_TOPIC=f1_drivers \
      -e KAFKA_POSITION_TOPIC=f1_position \
      -e KAFKA_LOCATION_TOPIC=f1_location \
      -e KAFKA_CAR_DATA_TOPIC=f1_car_data \
      -e CLICKHOUSE_URL=http://clickhouse:8123 \
      -e CLICKHOUSE_USERNAME=default \
      -e CLICKHOUSE_PASSWORD=test123 \
      -e CLICKHOUSE_DATABASE=default \
      -e CLICKHOUSE_DRIVERS_TABLE=drivers \
      -e CLICKHOUSE_CAR_DATA_TABLE=car_data \
      -e CLICKHOUSE_LOCATION_TABLE=location \
      -e CLICKHOUSE_POSITION_TABLE=position \
      jobmanager \
      flink run -d /tmp/real-time-f1.jar