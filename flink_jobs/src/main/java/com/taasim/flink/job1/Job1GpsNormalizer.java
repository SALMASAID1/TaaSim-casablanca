package com.taasim.flink.job1;

import com.taasim.flink.job1.functions.ParseGpsEventFn;
import com.taasim.flink.job1.functions.ValidationAndLateFilterFn;
import com.taasim.flink.job1.functions.ZoneMappingBroadcastFn;
import com.taasim.flink.job1.model.GpsNormalizedEvent;
import com.taasim.flink.job1.model.GpsRawEvent;
import com.taasim.flink.job1.model.ZoneDefinition;
import com.taasim.flink.job1.util.ZoneMappingLoader;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.List;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Job1GpsNormalizer {
    private static final String DEFAULT_KAFKA_BOOTSTRAP = "kafka:29092";
    private static final String DEFAULT_SOURCE_TOPIC = "raw.gps";
    private static final String DEFAULT_SINK_TOPIC = "processed.gps";
    private static final String DEFAULT_CASSANDRA_HOST = "cassandra";
    private static final int DEFAULT_CASSANDRA_PORT = 9042;
    private static final String DEFAULT_CITY = "casablanca";
    private static final String DEFAULT_CHECKPOINT_DIR =
            "s3a://taasim/raw/kafka-archive/flink-checkpoints/job1/";

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        buildJob(env, params);

        env.execute("job1-gps-normalizer");
    }

        static void buildJob(StreamExecutionEnvironment env, ParameterTool params) throws Exception {
        if (env == null) {
            throw new IllegalArgumentException("env must not be null");
        }
        if (params == null) {
            throw new IllegalArgumentException("params must not be null");
        }

        final String kafkaBootstrap = params.get("kafka-bootstrap-servers", DEFAULT_KAFKA_BOOTSTRAP);
        final String sourceTopic = params.get("source-topic", DEFAULT_SOURCE_TOPIC);
        final String sinkTopic = params.get("sink-topic", DEFAULT_SINK_TOPIC);
        final String cassandraHost = params.get("cassandra-host", DEFAULT_CASSANDRA_HOST);
        final int cassandraPort = params.getInt("cassandra-port", DEFAULT_CASSANDRA_PORT);
        final String city = params.get("city", DEFAULT_CITY);
        final String checkpointDir = params.get("checkpoint-dir", DEFAULT_CHECKPOINT_DIR);
        final long checkpointIntervalMs = params.getLong("checkpoint-interval-ms", 60_000L);

        env.getConfig().setGlobalJobParameters(params);
        env.getConfig().setAutoWatermarkInterval(1_000L);

        env.enableCheckpointing(checkpointIntervalMs);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
        env.getCheckpointConfig().setCheckpointStorage(checkpointDir);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30_000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.setStateBackend(new EmbeddedRocksDBStateBackend());

        final KafkaSource<String> rawGpsSource =
                KafkaSource.<String>builder()
                        .setBootstrapServers(kafkaBootstrap)
                        .setTopics(sourceTopic)
                        .setGroupId("flink-job1-gps")
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setValueOnlyDeserializer(new SimpleStringSchema())
                        .build();

        final DataStream<String> rawJson =
                env.fromSource(rawGpsSource, WatermarkStrategy.noWatermarks(), "Kafka raw.gps");

        final DataStream<GpsRawEvent> parsed = rawJson.flatMap(new ParseGpsEventFn()).name("parse-json");

        final WatermarkStrategy<GpsRawEvent> watermarkStrategy =
                WatermarkStrategy.<GpsRawEvent>forBoundedOutOfOrderness(Duration.ofMinutes(3))
                        .withTimestampAssigner((event, ts) -> event.eventTimeMillis);

        final DataStream<GpsRawEvent> watermarked =
                parsed.assignTimestampsAndWatermarks(watermarkStrategy).name("watermarks");

        final SingleOutputStreamOperator<GpsRawEvent> validated =
                watermarked.process(new ValidationAndLateFilterFn()).name("validate-and-late-filter");

        final List<ZoneDefinition> zones = ZoneMappingLoader.loadZonesFromClasspath();
        final DataStream<ZoneDefinition> zoneStream =
                zones.isEmpty()
                        ? env.fromElements(new ZoneDefinition()).filter(z -> false).name("zone-mapping-empty")
                        : env.fromCollection(zones).name("zone-mapping");

        final MapStateDescriptor<Integer, ZoneDefinition> zoneStateDesc =
                new MapStateDescriptor<>(
                        "zones",
                        Types.INT,
                        TypeInformation.of(new TypeHint<ZoneDefinition>() {}));

        final BroadcastStream<ZoneDefinition> zoneBroadcast = zoneStream.broadcast(zoneStateDesc);

        final DataStream<GpsNormalizedEvent> normalized =
                validated
                        .connect(zoneBroadcast)
                        .process(new ZoneMappingBroadcastFn(zoneStateDesc, city))
                        .name("zone-map-and-anonymize");

        final DataStream<Tuple8<String, Integer, Timestamp, String, Double, Double, Float, String>> cassandraRows =
                normalized
                        .map(
                                e ->
                                        Tuple8.of(
                                                e.city,
                                                e.zoneId,
                                                new Timestamp(e.eventTimeMillis),
                                                e.taxiId,
                                                e.lat,
                                                e.lon,
                                                e.speedKmh,
                                                e.status))
                        .returns(
                                Types.TUPLE(
                                        Types.STRING,
                                        Types.INT,
                                        Types.SQL_TIMESTAMP,
                                        Types.STRING,
                                        Types.DOUBLE,
                                        Types.DOUBLE,
                                        Types.FLOAT,
                                        Types.STRING))
                        .name("to-cassandra-rows");

        final String insertCql =
                "INSERT INTO taasim.vehicle_positions (city, zone_id, event_time, taxi_id, lat, lon, speed, status) "
                        + "VALUES (?, ?, ?, ?, ?, ?, ?, ?);";

        CassandraSink.addSink(cassandraRows).setQuery(insertCql).setHost(cassandraHost, cassandraPort).build();

        final SerializationSchema<Tuple2<String, String>> kafkaKeySerializer =
                element -> element.f0.getBytes(StandardCharsets.UTF_8);
        final SerializationSchema<Tuple2<String, String>> kafkaValueSerializer =
                element -> element.f1.getBytes(StandardCharsets.UTF_8);

        final KafkaSink<Tuple2<String, String>> processedGpsSink =
                KafkaSink.<Tuple2<String, String>>builder()
                        .setBootstrapServers(kafkaBootstrap)
                        .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                        .setRecordSerializer(
                                KafkaRecordSerializationSchema.<Tuple2<String, String>>builder()
                                        .setTopic(sinkTopic)
                                        .setKeySerializationSchema(kafkaKeySerializer)
                                        .setValueSerializationSchema(kafkaValueSerializer)
                                        .build())
                        .build();

        normalized
                .map(e -> Tuple2.of(e.taxiId, e.toJson()))
                .returns(Types.TUPLE(Types.STRING, Types.STRING))
                .name("to-processed-gps-json")
                .sinkTo(processedGpsSink)
                .name("kafka-processed.gps");
    }
}
