# Stream Processing with Apache Flink: F1 Race Analysis

This project replays Formula 1 telemetry for the 2026 Miami Grand Prix and builds a real-time dashboard on top of it.

The pipeline pulls historical race data from the OpenF1 API, produces it into Kafka-compatible topics in Redpanda, processes and enriches the streams with Apache Flink, writes the results to ClickHouse, and visualizes the race state in Grafana.

The current setup focuses on:

- Driver metadata
- Car telemetry such as speed, gear, RPM, throttle, and brake
- Track location coordinates
- Race position data
- Real-time Grafana panels backed by ClickHouse queries

## Architecture

```text
OpenF1 API
   |
   v
Python producer
   |
   v
Redpanda topics
   |
   v
Apache Flink job
   |
   v
ClickHouse tables
   |
   v
Grafana dashboard
```

The producer fetches data for:

- `drivers`
- `position`
- `location`
- `car_data`

The Flink job consumes these topics, enriches telemetry with driver metadata, and writes JSON rows into ClickHouse tables. Grafana reads from ClickHouse and refreshes the dashboard every second.

## Stack

- Java 17
- Apache Flink 2.0.1
- Redpanda 24.3.7
- ClickHouse 24.1
- Grafana 11.3.0
- Grafana ClickHouse datasource plugin
- Python producer managed with `uv`
- OpenF1 API as the telemetry source
- Docker Compose for local infrastructure

## Main Services

- Flink UI: http://localhost:8081
- Grafana: http://localhost:3000
- Redpanda Console: http://localhost:8080
- ClickHouse HTTP endpoint: http://localhost:8123
- Redpanda external broker: `localhost:19092`


## Repository Structure

```text
.
|-- src/main/java/com/github/abyssnlp
|   |-- Main.java
|   `-- schemas
|-- producer
|   |-- producer/main.py
|   |-- producer/ch_tables.py
|   `-- producer/reseeker.py
|-- grafana
|   |-- dashboards/f1-race-analysis.json
|   `-- provisioning
|-- clickhouse/config
|-- docker-compose.yml
|-- Makefile
`-- pom.xml
```

## How to Run

### 1. Start the infrastructure

```bash
docker compose up -d
```

Wait until Redpanda is healthy:

```bash
docker compose ps
```

### 2. Build the Flink job

```bash
mvn clean package
```

Alternatively, if you're using IntelliJ Idea, you can use the Maven plugin to execute it.

This creates:

```text
target/real-time-f1-1.0.jar
```

### 3. Create ClickHouse tables

```bash
make create-tables
```

### 4. Run the Flink job

```bash
make run-job
```

### 5. Run the producer

```bash
make producer
```

The producer uses event-time replay for telemetry. It combines `position`, `location`, and `car_data`, sorts the records by their OpenF1 `date`, and emits them in timestamp order.

The default replay speed is `60x`. You can override it:

```bash
cd producer
F1_REPLAY_SPEED=120 uv run producer
```

Use `F1_REPLAY_SPEED=1` if you want the replay to follow the original event-time pace.

### Alternative: run the main flow

```bash
make run
```

This creates tables, submits the Flink job, and starts the producer.

## Dashboard

Open Grafana at:

```text
http://localhost:3000
```

The dashboard is provisioned from:

```text
grafana/dashboards/f1-race-analysis.json
```

It includes:

- Current driver positions
- Top driver cards
- Speed, gear, RPM, throttle, and brake panels
- Driver track locations from the `location` table
- A reference track trace built from historical location points

## How to Stop, Destroy, and Reset

### Stop the Flink job

```bash
make stop-job
```

### Drop ClickHouse tables

```bash
make drop-tables
```

### Delete Redpanda topics

```bash
make drop-topics
```

This removes:

- `f1_position`
- `f1_location`
- `f1_car_data`
- `f1_drivers`

### Reset offsets

```bash
make reset-offsets
```

### Full project reset

```bash
make destroy
```

This stops the Flink job, drops the ClickHouse tables, resets Kafka consumer offsets, and deletes the Redpanda topics.

If you want to remove Docker volumes as well:

```bash
docker compose down -v
```

Then start from the run steps again.

## Useful Commands

List Redpanda topics:

```bash
docker exec -it redpanda rpk topic list --brokers localhost:9092
```

Delete a topic manually:

```bash
docker exec -it redpanda rpk topic delete f1_location --brokers localhost:9092
```

List running Flink jobs:

```bash
docker compose exec jobmanager flink list -r
```

Query ClickHouse:

```bash
docker exec -it clickhouse clickhouse-client --user default --password test123
```

## Learnings

### Joining telemetry with driver metadata

Issue: I first joined telemetry streams to driver data using only `driver_number`.

Fix: I changed the join key to include `meeting_key`, `session_key`, and `driver_number`.

Why it works: Driver numbers are only meaningful within a race session. A composite key keeps the enrichment scoped to the correct meeting and session.

### Windowed joins were too fragile for reference data

Issue: My initial `DataStream.join()` approach needed telemetry and driver records to arrive in the same processing-time window.

Fix: I replaced the windowed join with a broadcast-state join. The driver stream is treated as reference data and broadcast to the enrichment operators.

Why it works: Driver metadata changes slowly compared to telemetry. Broadcast state lets each telemetry event use the latest known driver details without depending on short co-arrival windows.

### Kafka sources do not guarantee cross-topic ordering

Issue: Broadcasting driver data did not guarantee driver records were processed before telemetry records.

Fix: I used `KeyedBroadcastProcessFunction` and buffered unmatched telemetry in keyed `ListState` until the matching driver arrived.

Why it works: Flink reads Kafka topics concurrently. Buffering protects the pipeline from source ordering differences and prevents early telemetry events from being dropped.

### Position records were being dropped

Issue: Position enrichment only emitted records when driver metadata was already available. Records that arrived early had no fallback path.

Fix: I stored unmatched `PositionWithOffset` records in keyed state and flushed them after the matching driver record updated broadcast state.

Why it works: The stream no longer loses position events just because the dimension data arrived later.

### Keyed state clearing needed to be understood carefully

Issue: I was concerned that calling `pendingPositions.clear()` might clear pending records for every driver.

Fix: I keyed telemetry by `meeting_key|session_key|driver_number` before connecting it to the broadcast stream.

Why it works: `ListState` inside `KeyedBroadcastProcessFunction` is scoped to the current key. Clearing it only clears the pending records for that specific driver key.

### Kafka metadata was needed for ClickHouse deduplication

Issue: Kafka value-only deserialization discarded metadata such as partition and offset.

Fix: I switched to a custom `KafkaRecordDeserializationSchema` that receives the full `ConsumerRecord`.

Why it works: The Flink job can use Kafka partition and offset to create a deterministic `version` column for ClickHouse `ReplacingMergeTree` tables.

### Kafka offsets are partition-local

Issue: Using only `record.offset()` as a ClickHouse version could collide across Kafka partitions.

Fix: I built the version as:

```text
partition * 1_000_000_000_000 + offset
```

Why it works: Kafka offsets are monotonic inside one partition. Multiplying the partition gives each partition its own offset range.

### Java `Instant` needed custom JSON handling

Issue: The custom Kafka metadata deserializer failed when parsing OpenF1 timestamps into `java.time.Instant`.

Fix: I registered a custom Jackson `Instant` deserializer in the shared `ObjectMapper`.

Why it works: Flink uses shaded Jackson, and Java time support is not available by default in this setup. The custom parser handles timestamps with offsets and offset-less timestamps.

### ClickHouse schema had to match enriched output

Issue: Enriched JSON rows included driver fields that were not present in the ClickHouse telemetry tables.

Fix: I added `full_name`, `name_acronym`, `team_name`, and `headshot_url` to the `car_data`, `location`, and `position` tables.

Why it works: ClickHouse `JSONEachRow` expects inserted fields to match the table schema unless unknown fields are explicitly skipped. Keeping the schema aligned preserves the enriched data.

### `CREATE TABLE IF NOT EXISTS` is not a migration

Issue: Existing ClickHouse tables did not change after I updated the table definitions.

Fix: I dropped and recreated the tables during development when the schema changed.

Why it works: `IF NOT EXISTS` only creates missing tables. It does not alter tables that already exist.

### `ReplacingMergeTree` requires careful querying

Issue: Normal queries could show older duplicate rows after replays.

Fix: I used `FINAL` where correctness matters, and `argMax(..., date)` or `argMax(..., version)` for latest-record dashboard queries.

Why it works: `ReplacingMergeTree` removes older versions during background merges. Until that happens, queries need to explicitly select the latest row.

### Location coordinates can be negative

Issue: Location coordinates were initially typed as unsigned integers.

Fix: I changed `x`, `y`, and `z` to signed integer types.

Why it works: OpenF1 location coordinates can be negative. Signed types match the data and prevent insert failures.

### Rate limiting is not the same as event-time replay

Issue: I was rate limiting the producer, but `position`, `location`, and `car_data` progressed at different event timestamps.

Fix: I changed the producer to combine telemetry records, sort them by `date`, and emit them according to event-time deltas.

Why it works: This makes Kafka arrival order follow race time across telemetry topics, which keeps the dashboard updates more coherent.

### Grafana XY chart schema changed between versions

Issue: The driver location panel failed with a JavaScript error because the dashboard JSON used the older XY chart manual-series shape.

Fix: I updated the panel to use the Grafana 11 XY chart schema with field matcher objects for `TrackX`, `TrackY`, `DriverX`, and `DriverY`.

Why it works: Grafana 11 renders the panel with the v2 XY chart implementation, which expects matchers instead of plain field names.
