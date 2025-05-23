package com.github.abyssnlp;

import com.github.abyssnlp.functions.CustomerEnrichment;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class Main {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Map<String, String> envVars = System.getenv();
        String kafkaBootstrapServers = envVars.get("KAFKA_BOOTSTRAP_SERVERS");
        String kafkaGroupId = envVars.get("KAFKA_GROUP_ID");
        String kafkaTopic = envVars.get("KAFKA_TOPIC");
        String kafkaEnrichedTopic = envVars.get("KAFKA_ENRICHED_TOPIC");
        String postgresUrl = envVars.get("POSTGRES_URL");
        String postgresUser = envVars.get("POSTGRES_USER");
        String postgresPassword = envVars.get("POSTGRES_PASSWORD");

        env.enableCheckpointing(120000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
        env.getCheckpointConfig().setCheckpointTimeout(120000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);


        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(kafkaBootstrapServers)
                .setTopics(kafkaTopic)
                .setGroupId("tester_v1")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperty("enable.auto.commit", "false")
                .setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                .setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
                .build();

        DataStream<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // Stream enrichment
        DataStream<String> enrichedStream = AsyncDataStream.orderedWait(
                stream,
                new CustomerEnrichment(postgresUrl, postgresUser, postgresPassword),
                5000,
                TimeUnit.MILLISECONDS,
                100
        );

        enrichedStream.sinkTo(
                KafkaSink.<String>builder()
                        .setBootstrapServers(kafkaBootstrapServers)
                        .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                                .setTopic(kafkaEnrichedTopic)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build())
                        .build()
        );

        env.execute("Flink async datastream enrichment");
    }
}