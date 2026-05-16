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

.PHONY: reset-offsets
reset-offsets:
	@echo "Resetting Kafka consumer group offsets..."
	cd producer && . .venv/bin/activate && python producer/reseeker.py --group real-time-f1 --topic f1_location --offset 0 --execute && python producer/reseeker.py --group real-time-f1 --topic f1_car_data --offset 0 --execute && python producer/reseeker.py --group real-time-f1 --topic f1_drivers --offset 0 --execute

.PHONY: stop-job
stop-job:
	@echo "Stopping the job..."
	@docker compose exec jobmanager sh -c './bin/flink list -r | awk "/RUNNING/ {print \$4}" | xargs -r -n1 ./bin/flink cancel'

.PHONY: drop-tables
drop-tables:
	@echo "Dropping ClickHouse tables..."
	cd producer && . .venv/bin/activate && uv run ch_tables --drop

.PHONY: drop-topics
drop-topics:
	@echo "Deleting Kafka Topics..."
	@docker exec -it redpanda rpk topic delete \
      f1_position \
      f1_location \
      f1_car_data \
      f1_drivers \
      --brokers localhost:9092

.PHONY: destroy
destroy: stop-job drop-tables reset-offsets drop-topics
	@echo "Environment has been destroyed and reset!"

.PHONY: create-tables
create-tables:
	@echo "Creating ClickHouse tables..."
	cd producer && . .venv/bin/activate && uv run ch_tables --create

.PHONY: producer
producer:
	@echo "Running the producer..."
	cd producer && . .venv/bin/activate && uv run producer

.PHONY: run
run: create-tables run-job producer
	@echo "Environment is up and running!"
