package com.taasim.flink.job2;

import com.taasim.flink.job2.functions.DemandAggregateFunction;
import com.taasim.flink.job2.functions.ParseGpsProcessedEventFn;
import com.taasim.flink.job2.functions.ParseTripRequestEventFn;
import com.taasim.flink.job2.model.DemandZoneAggregate;
import com.taasim.flink.job2.model.GpsProcessedEvent;
import com.taasim.flink.job2.model.TripRequestEvent;
import com.taasim.flink.job2.model.UnifiedWindowInput;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Duration;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class Job2DemandAggregator {
    private static final String DEFAULT_KAFKA_BOOTSTRAP = "kafka:29092";
    private static final String DEFAULT_GPS_TOPIC = "processed.gps";
    private static final String DEFAULT_TRIPS_TOPIC = "raw.trips";
    private static final String DEFAULT_DEMAND_TOPIC = "processed.demand";
    private static final String DEFAULT_CASSANDRA_HOST = "cassandra";
    private static final int DEFAULT_CASSANDRA_PORT = 9042;
    private static final String DEFAULT_CHECKPOINT_DIR =
            "s3a://taasim/raw/kafka-archive/flink-checkpoints/job2/";

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        buildJob(env, params);

        env.execute("job2-demand-aggregator");
    }

    public static void buildJob(StreamExecutionEnvironment env, ParameterTool params) throws Exception {
        if (env == null) {
            throw new IllegalArgumentException("env must not be null");
        }
        if (params == null) {
            throw new IllegalArgumentException("params must not be null");
        }

        final String kafkaBootstrap = params.get("kafka-bootstrap-servers", DEFAULT_KAFKA_BOOTSTRAP);
        final String gpsSourceTopic = params.get("gps-source-topic", DEFAULT_GPS_TOPIC);
        final String tripsSourceTopic = params.get("trips-source-topic", DEFAULT_TRIPS_TOPIC);
        final String demandSinkTopic = params.get("demand-sink-topic", DEFAULT_DEMAND_TOPIC);
        final String cassandraHost = params.get("cassandra-host", DEFAULT_CASSANDRA_HOST);
        final int cassandraPort = params.getInt("cassandra-port", DEFAULT_CASSANDRA_PORT);
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

        // Sources
        final KafkaSource<String> gpsSource =
                KafkaSource.<String>builder()
                        .setBootstrapServers(kafkaBootstrap)
                        .setTopics(gpsSourceTopic)
                        .setGroupId("flink-job2-gps")
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setValueOnlyDeserializer(new SimpleStringSchema())
                        .setProperty("security.protocol", "SASL_PLAINTEXT")
                        .setProperty("sasl.mechanism", "PLAIN")
                        .setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"flink\" password=\"flink-secret\";")
                        .build();

        final KafkaSource<String> tripsSource =
                KafkaSource.<String>builder()
                        .setBootstrapServers(kafkaBootstrap)
                        .setTopics(tripsSourceTopic)
                        .setGroupId("flink-job2-trips")
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setValueOnlyDeserializer(new SimpleStringSchema())
                        .setProperty("security.protocol", "SASL_PLAINTEXT")
                        .setProperty("sasl.mechanism", "PLAIN")
                        .setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"flink\" password=\"flink-secret\";")
                        .build();

        final DataStream<String> gpsRawJson =
                env.fromSource(gpsSource, WatermarkStrategy.noWatermarks(), "Kafka processed.gps");
        final DataStream<String> tripsRawJson =
                env.fromSource(tripsSource, WatermarkStrategy.noWatermarks(), "Kafka raw.trips");

        // Parsing
        final DataStream<GpsProcessedEvent> gpsParsed =
                gpsRawJson.flatMap(new ParseGpsProcessedEventFn()).name("parse-gps-processed-json");
        final DataStream<TripRequestEvent> tripsParsed =
                tripsRawJson.flatMap(new ParseTripRequestEventFn()).name("parse-trip-request-json");

        // Watermarks
        final WatermarkStrategy<GpsProcessedEvent> gpsWatermarkStrategy =
                WatermarkStrategy.<GpsProcessedEvent>forBoundedOutOfOrderness(Duration.ofMinutes(3))
                        .withTimestampAssigner((event, ts) -> event.eventTimeMillis);

        final WatermarkStrategy<TripRequestEvent> tripsWatermarkStrategy =
                WatermarkStrategy.<TripRequestEvent>forBoundedOutOfOrderness(Duration.ofMinutes(3))
                        .withTimestampAssigner((event, ts) -> event.getRequestedAtMillis());

        final DataStream<GpsProcessedEvent> gpsWatermarked =
                gpsParsed.assignTimestampsAndWatermarks(gpsWatermarkStrategy).name("gps-watermarks");

        final DataStream<TripRequestEvent> tripsWatermarked =
                tripsParsed.assignTimestampsAndWatermarks(tripsWatermarkStrategy).name("trips-watermarks");

        // Map to Unified Type
        final DataStream<UnifiedWindowInput> gpsUnified =
                gpsWatermarked
                        .map(e -> {
                            UnifiedWindowInput u = new UnifiedWindowInput();
                            u.city = e.city;
                            u.zoneId = e.zoneId;
                            u.eventType = "VEHICLE";
                            u.entityId = e.taxiId;
                            u.eventTimeMillis = e.eventTimeMillis;
                            return u;
                        })
                        .returns(Types.POJO(UnifiedWindowInput.class))
                        .name("gps-to-unified");

        final DataStream<UnifiedWindowInput> tripsUnified =
                tripsWatermarked
                        .map(e -> {
                            UnifiedWindowInput u = new UnifiedWindowInput();
                            u.city = "casablanca";
                            u.zoneId = e.originZone;
                            u.eventType = "REQUEST";
                            u.entityId = e.tripId;
                            u.eventTimeMillis = e.getRequestedAtMillis();
                            return u;
                        })
                        .returns(Types.POJO(UnifiedWindowInput.class))
                        .name("trips-to-unified");

        // Stream Union & Keying
        final DataStream<UnifiedWindowInput> unioned = gpsUnified.union(tripsUnified);

        // Window Aggregation
        final SingleOutputStreamOperator<DemandZoneAggregate> aggregates =
                unioned.keyBy(e -> e.zoneId)
                        .window(TumblingEventTimeWindows.of(Time.seconds(30)))
                        .process(new DemandAggregateFunction())
                        .name("demand-aggregator-30s-window");

        // Cassandra Sink
        final DataStream<Tuple7<String, Integer, Timestamp, Integer, Integer, Float, Float>> cassandraRows =
                aggregates
                        .map(e -> Tuple7.of(
                                e.city,
                                e.zoneId,
                                new Timestamp(e.windowStart),
                                e.activeVehicles,
                                e.pendingRequests,
                                e.ratio,
                                e.forecastDemand
                        ))
                        .returns(Types.TUPLE(
                                Types.STRING,
                                Types.INT,
                                Types.SQL_TIMESTAMP,
                                Types.INT,
                                Types.INT,
                                Types.FLOAT,
                                Types.FLOAT
                        ))
                        .name("to-cassandra-rows");

        final String insertCql =
                "INSERT INTO taasim.demand_zones (city, zone_id, window_start, active_vehicles, pending_requests, ratio, forecast_demand) "
                        + "VALUES (?, ?, ?, ?, ?, ?, ?);";

        CassandraSink.addSink(cassandraRows)
                .setQuery(insertCql)
                .setHost(cassandraHost, cassandraPort)
                .build();

        // Kafka Sink
        final SerializationSchema<Tuple2<String, String>> kafkaKeySerializer =
                element -> element.f0.getBytes(StandardCharsets.UTF_8);
        final SerializationSchema<Tuple2<String, String>> kafkaValueSerializer =
                element -> element.f1.getBytes(StandardCharsets.UTF_8);

        final KafkaSink<Tuple2<String, String>> demandSink =
                KafkaSink.<Tuple2<String, String>>builder()
                        .setBootstrapServers(kafkaBootstrap)
                        .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                        .setProperty("security.protocol", "SASL_PLAINTEXT")
                        .setProperty("sasl.mechanism", "PLAIN")
                        .setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"flink\" password=\"flink-secret\";")
                        .setRecordSerializer(
                                KafkaRecordSerializationSchema.<Tuple2<String, String>>builder()
                                        .setTopic(demandSinkTopic)
                                        .setKeySerializationSchema(kafkaKeySerializer)
                                        .setValueSerializationSchema(kafkaValueSerializer)
                                        .build())
                        .build();

        aggregates
                .map(e -> Tuple2.of(String.valueOf(e.zoneId), e.toJson()))
                .returns(Types.TUPLE(Types.STRING, Types.STRING))
                .name("to-demand-json")
                .sinkTo(demandSink)
                .name("kafka-processed.demand");
    }
}
