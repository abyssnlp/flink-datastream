Stream Processing with Apache Flink: F1 Race Analysis
=========================================================

## Learnings

- Issue: Joining telemetry streams to `driversStream` only on `driver_number` could attach the wrong driver metadata when multiple meetings or sessions are present.
  Fix: Use a composite join key made from `meeting_key`, `session_key`, and `driver_number`.
  Why: `driver_number` is only unique within a session context, while the ClickHouse dedup key and source data are scoped by meeting and session.

- Issue: The initial `DataStream.join()` approach required telemetry and driver records to arrive in the same processing-time window.
  Fix: Replace the windowed joins with a broadcast-state join where `driversStream` is broadcast as dimension data.
  Why: Driver records are reference data, so enrichment should use the latest known driver state instead of depending on co-arrival in a short time window.

- Issue: Broadcasting `driversStream` did not guarantee it would be processed before telemetry streams.
  Fix: Use `KeyedBroadcastProcessFunction` and buffer unmatched telemetry records in keyed `ListState` until the matching driver arrives.
  Why: Flink runs Kafka sources concurrently and does not provide source ordering guarantees across topics in one unbounded job.

- Issue: Position enrichment emitted only one driver's records because position events that arrived before driver state existed were silently dropped.
  Fix: Store unmatched `PositionWithOffset` records in keyed state and flush them in `processBroadcastElement` when the corresponding driver record updates broadcast state.
  Why: The previous `if (driver != null) out.collect(...)` logic had no else path, so missing dimension lookups caused data loss.

- Issue: There was concern that `pendingPositions.clear()` might remove pending records for all drivers.
  Fix: Key the telemetry stream by `meeting_key|session_key|driver_number` before connecting to the broadcast stream.
  Why: `ListState` inside `KeyedBroadcastProcessFunction` is scoped per keyed stream key, so clearing it only removes pending records for the matched driver key.

- Issue: Kafka value-only deserialization discarded topic metadata such as partition and offset.
  Fix: Use a generic `KafkaRecordDeserializationSchema` that decodes the JSON value and passes the full `ConsumerRecord` into a mapper.
  Why: The Kafka offset and partition are needed to derive a deterministic `version` for ClickHouse `ReplacingMergeTree` rows.

- Issue: Using only `record.offset()` as `version` can collide across Kafka partitions.
  Fix: Compose the version as `record.partition() * 1_000_000_000_000L + record.offset()`.
  Why: Kafka offsets are monotonic only within a partition, not across an entire topic.

- Issue: The first partition-offset formula used addition instead of multiplication.
  Fix: Change `record.partition() + 1_000_000_000_000L + record.offset()` to `record.partition() * 1_000_000_000_000L + record.offset()`.
  Why: Addition can collide between adjacent partitions and offsets; multiplication gives each partition its own offset range.

- Issue: The custom Kafka metadata deserializer failed on `java.time.Instant`.
  Fix: Register a custom Jackson `Instant` deserializer in the shared `ObjectMapper`.
  Why: Flink's shaded Jackson mapper does not support Java 8 date/time types by default without an additional Java time module.

- Issue: Enriched JSON rows initially included driver columns that were not present in the ClickHouse telemetry tables.
  Fix: Add `full_name`, `name_acronym`, `team_name`, and `headshot_url` columns to the `car_data`, `location`, and `position` tables.
  Why: ClickHouse `JSONEachRow` rejects unknown fields unless configured to skip them, and skipping them would discard the enrichment.

- Issue: Existing ClickHouse tables are not changed by `CREATE TABLE IF NOT EXISTS`.
  Fix: Drop and recreate the tables or run explicit `ALTER TABLE ADD COLUMN` statements when the schema changes.
  Why: `IF NOT EXISTS` only creates missing tables; it does not migrate existing table definitions.

- Issue: Querying a `ReplacingMergeTree(version)` table can show older duplicate rows.
  Fix: Use `FINAL` for correctness, or use `argMax(..., version)` queries for performance-sensitive latest-record reads.
  Why: `ReplacingMergeTree` removes older versions asynchronously during merges, so normal queries may see multiple versions until merges complete.

- Issue: Location coordinates were typed as unsigned integers in ClickHouse.
  Fix: Change `x`, `y`, and `z` to signed integer types.
  Why: Location coordinates can be negative, and inserting negative values into `UInt32` columns will fail.
