package com.github.abyssnlp;

import com.clickhouse.data.ClickHouseFormat;
import com.github.abyssnlp.schemas.*;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.clickhouse.data.ClickHousePayload;
import org.apache.flink.connector.clickhouse.sink.ClickHouseAsyncSink;
import org.apache.flink.connector.clickhouse.sink.ClickHouseClientConfig;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonDeserializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.Map;


public class Main {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Map<String, String> envVars = System.getenv();

        String kafkaBootstrapServers = envVars.get("KAFKA_BOOTSTRAP_SERVERS");
        String kafkaGroupId = envVars.get("KAFKA_GROUP_ID");
        String driversTopic = envVars.get("KAFKA_DRIVERS_TOPIC");
        String positionTopic = envVars.get("KAFKA_POSITION_TOPIC");
        String locationTopic = envVars.get("KAFKA_LOCATION_TOPIC");
        String carDataTopic = envVars.get("KAFKA_CAR_DATA_TOPIC");
        String clickHouseUrl = envVars.get("CLICKHOUSE_URL");
        String clickHouseUsername = envVars.get("CLICKHOUSE_USERNAME");
        String clickHousePassword = envVars.get("CLICKHOUSE_PASSWORD");
        String clickHouseDatabase = envVars.get("CLICKHOUSE_DATABASE");
        String driversTable = envVars.get("CLICKHOUSE_DRIVERS_TABLE");
        String carDataTable = envVars.get("CLICKHOUSE_CAR_DATA_TABLE");
        String locationTable = envVars.get("CLICKHOUSE_LOCATION_TABLE");
        String positionTable = envVars.get("CLICKHOUSE_POSITION_TABLE");


        env.enableCheckpointing(120000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
        env.getCheckpointConfig().setCheckpointTimeout(120000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);

        DataStream<Driver> driversStream = getDataStreamFromTopic(
                driversTopic,
                kafkaBootstrapServers,
                kafkaGroupId,
                Driver.class,
                "drivers_stream",
                env
        );

        DataStream<CarDataWithOffset> carDataStream = getDataStreamFromTopicWithMetadata(
                carDataTopic,
                kafkaBootstrapServers,
                kafkaGroupId,
                CarData.class,
                TypeInformation.of(CarDataWithOffset.class),
                (carData, record) -> new CarDataWithOffset(
                        carData,
                        record.partition() * 1_000_000_000_000L + record.offset()
                ),
                "car_data_stream",
                env
        );

        DataStream<LocationWithOffset> locationStream = getDataStreamFromTopicWithMetadata(
                locationTopic,
                kafkaBootstrapServers,
                kafkaGroupId,
                Location.class,
                TypeInformation.of(LocationWithOffset.class),
                (location, record) -> new LocationWithOffset(location, record.partition() * 1_000_000_000_000L + record.offset()),
                "location_stream",
                env
        );

        DataStream<PositionWithOffset> positionStream = getDataStreamFromTopicWithMetadata(
                positionTopic,
                kafkaBootstrapServers,
                kafkaGroupId,
                Position.class,
                TypeInformation.of(PositionWithOffset.class),
                (position, record) -> new PositionWithOffset(position, record.partition() * 1_000_000_000_000L + record.offset()),
                "position_stream",
                env
        );

        // Drivers
        driversStream
                .sinkTo(createClickHouseJsonSink(
                        clickHouseUrl,
                        clickHouseUsername,
                        clickHousePassword,
                        clickHouseDatabase,
                        driversTable,
                        Driver::toClickHouseJson
                ))
                .name("F1 Drivers Sink");

        MapStateDescriptor<String, Driver> driversBroadcastStateDescriptor =
                new MapStateDescriptor<>("drivers_by_meeting_session_number", String.class, Driver.class);

        BroadcastStream<Driver> broadcastDriversStream =
                driversStream.broadcast(driversBroadcastStateDescriptor);

        // Car Data + Driver
        DataStream<EnrichedCarData> enrichedCarDataDataStream = carDataStream
                .keyBy((record) -> driverJoinKey(
                        record.carData().meeting_key(),
                        record.carData().session_key(),
                        record.carData().driver_number()
                ))
                .connect(broadcastDriversStream)
                .process(new KeyedBroadcastProcessFunction<String, CarDataWithOffset, Driver, EnrichedCarData>() {
                    private final ListStateDescriptor<CarDataWithOffset> pendingCarDataDescriptor =
                            new ListStateDescriptor<>(
                                    "pending_car_data",
                                    TypeInformation.of(CarDataWithOffset.class)
                            );

                    @Override
                    public void processElement(
                            CarDataWithOffset record,
                            ReadOnlyContext context,
                            Collector<EnrichedCarData> out
                    ) throws Exception {
                        CarData car = record.carData();
                        ReadOnlyBroadcastState<String, Driver> driversState =
                                context.getBroadcastState(driversBroadcastStateDescriptor);
                        Driver driver = driversState.get(driverJoinKey(
                                car.meeting_key(),
                                car.session_key(),
                                car.driver_number()
                        ));

                        if (driver != null) {
                            out.collect(enrichCarData(record, driver));
                        } else {
                            getRuntimeContext().getListState(pendingCarDataDescriptor).add(record);
                        }
                    }

                    @Override
                    public void processBroadcastElement(
                            Driver driver,
                            Context context,
                            Collector<EnrichedCarData> out
                    ) throws Exception {
                        context.getBroadcastState(driversBroadcastStateDescriptor)
                                .put(driverJoinKey(
                                        driver.meeting_key(),
                                        driver.session_key(),
                                        driver.driver_number()
                                ), driver);

                        String driverKey = driverJoinKey(
                                driver.meeting_key(),
                                driver.session_key(),
                                driver.driver_number()
                        );

                        context.applyToKeyedState(pendingCarDataDescriptor, (key, pendingCarData) -> {
                            if (driverKey.equals(key)) {
                                for (CarDataWithOffset pendingRecord : pendingCarData.get()) {
                                    out.collect(enrichCarData(pendingRecord, driver));
                                }
                                pendingCarData.clear();
                            }
                        });
                    }
                })
                .name("Enrich Car Data With Drivers");

        // Location + Driver
        DataStream<EnrichedLocation> enrichedLocationDataStream = locationStream
                .keyBy((record) -> driverJoinKey(
                        record.location().meeting_key(),
                        record.location().session_key(),
                        record.location().driver_number()
                ))
                .connect(broadcastDriversStream)
                .process(new KeyedBroadcastProcessFunction<String, LocationWithOffset, Driver, EnrichedLocation>() {
                    private final ListStateDescriptor<LocationWithOffset> pendingLocationDescriptor =
                            new ListStateDescriptor<>(
                                    "pending_location",
                                    TypeInformation.of(LocationWithOffset.class)
                            );

                    @Override
                    public void processElement(
                            LocationWithOffset record,
                            ReadOnlyContext context,
                            Collector<EnrichedLocation> out
                    ) throws Exception {
                        Location location = record.location();
                        ReadOnlyBroadcastState<String, Driver> driversState =
                                context.getBroadcastState(driversBroadcastStateDescriptor);
                        Driver driver = driversState.get(driverJoinKey(
                                location.meeting_key(),
                                location.session_key(),
                                location.driver_number()
                        ));

                        if (driver != null) {
                            out.collect(enrichLocation(record, driver));
                        } else {
                            getRuntimeContext().getListState(pendingLocationDescriptor).add(record);
                        }
                    }

                    @Override
                    public void processBroadcastElement(
                            Driver driver,
                            Context context,
                            Collector<EnrichedLocation> out
                    ) throws Exception {
                        context.getBroadcastState(driversBroadcastStateDescriptor)
                                .put(driverJoinKey(
                                        driver.meeting_key(),
                                        driver.session_key(),
                                        driver.driver_number()
                                ), driver);

                        String driverKey = driverJoinKey(
                                driver.meeting_key(),
                                driver.session_key(),
                                driver.driver_number()
                        );

                        context.applyToKeyedState(pendingLocationDescriptor, (key, pendingLocations) -> {
                            if (driverKey.equals(key)) {
                                for (LocationWithOffset pendingRecord : pendingLocations.get()) {
                                    out.collect(enrichLocation(pendingRecord, driver));
                                }
                                pendingLocations.clear();
                            }
                        });
                    }
                })
                .name("Enrich Location With Drivers");

        // Position + Driver
        DataStream<EnrichedPosition> enrichedPositionDataStream = positionStream
                .keyBy((record) -> driverJoinKey(
                        record.position().meeting_key(),
                        record.position().session_key(),
                        record.position().driver_number()
                ))
                .connect(broadcastDriversStream)
                .process(new KeyedBroadcastProcessFunction<String, PositionWithOffset, Driver, EnrichedPosition>() {
                    private final ListStateDescriptor<PositionWithOffset> pendingPositionDescriptor =
                            new ListStateDescriptor<>(
                                    "pending_position",
                                    TypeInformation.of(PositionWithOffset.class)
                            );

                    @Override
                    public void processElement(
                            PositionWithOffset record,
                            ReadOnlyContext context,
                            Collector<EnrichedPosition> out
                    ) throws Exception {
                        Position position = record.position();
                        ReadOnlyBroadcastState<String, Driver> driversState =
                                context.getBroadcastState(driversBroadcastStateDescriptor);
                        Driver driver = driversState.get(driverJoinKey(
                                position.meeting_key(),
                                position.session_key(),
                                position.driver_number()
                        ));

                        if (driver != null) {
                            out.collect(enrichPosition(record, driver));
                        } else {
                            getRuntimeContext().getListState(pendingPositionDescriptor).add(record);
                        }
                    }

                    @Override
                    public void processBroadcastElement(
                            Driver driver,
                            Context context,
                            Collector<EnrichedPosition> out
                    ) throws Exception {
                        context.getBroadcastState(driversBroadcastStateDescriptor)
                                .put(driverJoinKey(
                                        driver.meeting_key(),
                                        driver.session_key(),
                                        driver.driver_number()
                                ), driver);

                        String driverKey = driverJoinKey(
                                driver.meeting_key(),
                                driver.session_key(),
                                driver.driver_number()
                        );

                        context.applyToKeyedState(pendingPositionDescriptor, (key, pendingPositions) -> {
                            if (driverKey.equals(key)) {
                                for (PositionWithOffset pendingRecord : pendingPositions.get()) {
                                    out.collect(enrichPosition(pendingRecord, driver));
                                }
                                pendingPositions.clear();
                            }
                        });
                    }
                })
                .name("Enrich Position With Drivers");

        enrichedCarDataDataStream
                .sinkTo(createClickHouseJsonSink(
                        clickHouseUrl,
                        clickHouseUsername,
                        clickHousePassword,
                        clickHouseDatabase,
                        carDataTable,
                        EnrichedCarData::toClickHouseJson
                ))
                .name("Enriched Car Data Sink");

        enrichedLocationDataStream
                .sinkTo(createClickHouseJsonSink(
                        clickHouseUrl,
                        clickHouseUsername,
                        clickHousePassword,
                        clickHouseDatabase,
                        locationTable,
                        EnrichedLocation::toClickHouseJson
                ))
                .name("Enriched Location Sink");

        enrichedPositionDataStream
                .sinkTo(createClickHouseJsonSink(
                        clickHouseUrl,
                        clickHouseUsername,
                        clickHousePassword,
                        clickHouseDatabase,
                        positionTable,
                        EnrichedPosition::toClickHouseJson
                ))
                .name("Enriched Position Sink");

        env.execute();

    }

    private static EnrichedCarData enrichCarData(CarDataWithOffset record, Driver driver) {
        CarData car = record.carData();
        return new EnrichedCarData(
                car.date(),
                car.driver_number(),
                car.session_key(),
                car.meeting_key(),
                car.speed(),
                car.n_gear(),
                car.rpm(),
                car.throttle(),
                car.brake(),
                driver.full_name(),
                driver.name_acronym(),
                driver.team_name(),
                driver.headshot_url(),
                record.offset()
        );
    }

    private static EnrichedLocation enrichLocation(LocationWithOffset record, Driver driver) {
        Location location = record.location();
        return new EnrichedLocation(
                location.date(),
                location.driver_number(),
                location.session_key(),
                location.meeting_key(),
                location.x(),
                location.y(),
                location.z(),
                driver.full_name(),
                driver.name_acronym(),
                driver.team_name(),
                driver.headshot_url(),
                record.offset()
        );
    }

    private static EnrichedPosition enrichPosition(PositionWithOffset record, Driver driver) {
        Position position = record.position();
        return new EnrichedPosition(
                position.date(),
                position.session_key(),
                position.meeting_key(),
                position.driver_number(),
                position.position(),
                driver.full_name(),
                driver.name_acronym(),
                driver.team_name(),
                driver.headshot_url(),
                record.offset()
        );
    }

    private static String driverJoinKey(
            long meetingKey,
            long sessionKey,
            int driverNumber
    ) {
        return meetingKey + "|" + sessionKey + "|" + driverNumber;
    }

    static <T> DataStream<T> getDataStreamFromTopic(
            String topic,
            String bootstrapServers,
            String consumerGroupId,
            Class<T> valueClass,
            String name,
            StreamExecutionEnvironment env

    ) {
        KafkaSource<T> source = KafkaSource.<T>builder()
                .setBootstrapServers(bootstrapServers)
                .setGroupId(consumerGroupId)
                .setTopics(topic)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setProperty("enable.auto.commit", "false")
                .setValueOnlyDeserializer(new JsonDeserializationSchema<>(
                        valueClass,
                        Main::createObjectMapper
                ))
                .build();

        return env
                .fromSource(source, WatermarkStrategy.noWatermarks(), name)
                .uid(name)
                .name(name);
    }

    static <T, OUT> DataStream<OUT> getDataStreamFromTopicWithMetadata(
            String topic,
            String bootstrapServers,
            String consumerGroupId,
            Class<T> valueClass,
            TypeInformation<OUT> producedType,
            KafkaRecordMapper<T, OUT> mapper,
            String name,
            StreamExecutionEnvironment env
    ) {
        KafkaSource<OUT> source = KafkaSource.<OUT>builder()
                .setBootstrapServers(bootstrapServers)
                .setGroupId(consumerGroupId)
                .setTopics(topic)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setProperty("enable.auto.commit", "false")
                .setDeserializer(jsonWithKafkaMetadata(valueClass, producedType, mapper))
                .build();

        return env
                .fromSource(source, WatermarkStrategy.noWatermarks(), name)
                .uid(name)
                .name(name);
    }

    private static <T> ClickHouseAsyncSink<T> createClickHouseJsonSink(
            String clickHouseUrl,
            String clickHouseUsername,
            String clickHousePassword,
            String clickHouseDatabase,
            String tableName,
            JsonFormatter<T> jsonFormatter
    ) {
        ClickHouseClientConfig clickHouseConfig = new ClickHouseClientConfig(
                clickHouseUrl,
                clickHouseUsername,
                clickHousePassword,
                clickHouseDatabase,
                tableName
        );

        ElementConverter<T, ClickHousePayload> converter = (record, context) ->
                new ClickHousePayload(jsonFormatter.toJson(record).getBytes(StandardCharsets.UTF_8));

        ClickHouseAsyncSink<T> sink = new ClickHouseAsyncSink<>(
                converter,
                1000,
                3,
                10000,
                5_000_000,
                1000,
                1_000_000,
                clickHouseConfig
        );

        sink.setClickHouseFormat(ClickHouseFormat.JSONEachRow);
        return sink;
    }

    private static <T, OUT> KafkaRecordDeserializationSchema<OUT> jsonWithKafkaMetadata(
            Class<T> valueClass,
            TypeInformation<OUT> producedType,
            KafkaRecordMapper<T, OUT> mapper
    ) {
        return new KafkaRecordDeserializationSchema<>() {
            private transient ObjectMapper objectMapper;

            @Override
            public void open(DeserializationSchema.InitializationContext context) {
                objectMapper = createObjectMapper();
            }

            @Override
            public void deserialize(
                    ConsumerRecord<byte[], byte[]> record,
                    Collector<OUT> out
            ) throws IOException {
                T value = objectMapper.readValue(record.value(), valueClass);
                out.collect(mapper.map(value, record));
            }

            @Override
            public TypeInformation<OUT> getProducedType() {
                return producedType;
            }
        };
    }

    private static ObjectMapper createObjectMapper() {
        SimpleModule javaTimeModule = new SimpleModule();
        javaTimeModule.addDeserializer(Instant.class, new JsonDeserializer<>() {
            @Override
            public Instant deserialize(JsonParser parser, DeserializationContext context) throws IOException {
                String value = parser.getValueAsString();
                if (value == null || value.isBlank()) {
                    return null;
                }

                try {
                    return Instant.parse(value);
                } catch (DateTimeParseException ignored) {
                    // OpenF1 timestamps commonly include offsets like +00:00.
                }

                try {
                    return OffsetDateTime.parse(value).toInstant();
                } catch (DateTimeParseException ignored) {
                    // Treat offset-less timestamps as UTC.
                }

                return LocalDateTime.parse(value).toInstant(ZoneOffset.UTC);
            }
        });

        return new ObjectMapper()
                .registerModule(javaTimeModule)
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @FunctionalInterface
    private interface JsonFormatter<T> extends Serializable {
        String toJson(T value);
    }

    @FunctionalInterface
    private interface KafkaRecordMapper<T, OUT> extends Serializable {
        OUT map(T value, ConsumerRecord<byte[], byte[]> record);
    }

}
