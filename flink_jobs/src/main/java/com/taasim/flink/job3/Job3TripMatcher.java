package com.taasim.flink.job3;

import com.taasim.flink.job3.functions.ParseGpsProcessedEventFn;
import com.taasim.flink.job3.functions.ParseTripRequestEventFn;
import com.taasim.flink.job3.functions.TripMatcherFunction;
import com.taasim.flink.job3.model.GpsProcessedEvent;
import com.taasim.flink.job3.model.TripMatchEvent;
import com.taasim.flink.job3.model.TripRequestEvent;
import com.taasim.flink.job3.model.VehicleInfo;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple12;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Job3TripMatcher {
    private static final String DEFAULT_KAFKA_BOOTSTRAP = "kafka:29092";
    private static final String DEFAULT_GPS_TOPIC = "processed.gps";
    private static final String DEFAULT_TRIPS_TOPIC = "raw.trips";
    private static final String DEFAULT_UNMATCHED_TOPIC = "raw.unmatched";
    private static final String DEFAULT_CASSANDRA_HOST = "cassandra";
    private static final int DEFAULT_CASSANDRA_PORT = 9042;
    private static final String DEFAULT_CHECKPOINT_DIR =
            "s3a://taasim/raw/kafka-archive/flink-checkpoints/job3/";

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        buildJob(env, params);

        env.execute("job3-trip-matcher");
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
        final String unmatchedSinkTopic = params.get("unmatched-sink-topic", DEFAULT_UNMATCHED_TOPIC);
        final String cassandraHost = params.get("cassandra-host", DEFAULT_CASSANDRA_HOST);
        final int cassandraPort = params.getInt("cassandra-port", DEFAULT_CASSANDRA_PORT);
        final String checkpointDir = params.get("checkpoint-dir", DEFAULT_CHECKPOINT_DIR);
        final long checkpointIntervalMs = params.getLong("checkpoint-interval-ms", 60_000L);

        env.getConfig().setGlobalJobParameters(params);
        env.getConfig().setAutoWatermarkInterval(1_000L);

        env.enableCheckpointing(checkpointIntervalMs);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
        env.getCheckpointConfig().setCheckpointStorage(checkpointDir);

        // Add Restart Strategy for Chaos Engineering (Automatic Recovery)
        env.setRestartStrategy(org.apache.flink.api.common.restartstrategy.RestartStrategies.fixedDelayRestart(
            3, // number of restart attempts
            org.apache.flink.api.common.time.Time.seconds(10) // delay between attempts
        ));
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30_000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.setStateBackend(new EmbeddedRocksDBStateBackend());

        // Kafka Sources
        final KafkaSource<String> gpsSource =
                KafkaSource.<String>builder()
                        .setBootstrapServers(kafkaBootstrap)
                        .setTopics(gpsSourceTopic)
                        .setGroupId("flink-job3-gps")
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
                        .setGroupId("flink-job3-trips")
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

        // Parse Json
        final DataStream<GpsProcessedEvent> gpsParsed =
                gpsRawJson.flatMap(new ParseGpsProcessedEventFn()).name("parse-gps-processed-json");
        final DataStream<TripRequestEvent> tripsParsed =
                tripsRawJson.flatMap(new ParseTripRequestEventFn()).name("parse-trip-request-json");

        // Watermarks (3 minutes BoundedOutOfOrderness)
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

        // Broadcast State Descriptor
        final MapStateDescriptor<String, VehicleInfo> vehicleStateDesc =
                new MapStateDescriptor<>(
                        "vehicles",
                        Types.STRING,
                        TypeInformation.of(VehicleInfo.class)
                );

        final BroadcastStream<GpsProcessedEvent> broadcastGps = gpsWatermarked.broadcast(vehicleStateDesc);

        // Core STATEFUL co-process matching
        final SingleOutputStreamOperator<TripMatchEvent> matches =
                tripsWatermarked
                        .keyBy(event -> event.originZone)
                        .connect(broadcastGps)
                        .process(new TripMatcherFunction(vehicleStateDesc))
                        .name("trip-matcher-process-function");

        // Map Matches to Cassandra Rows (Tuple12)
        final DataStream<Tuple12<String, com.datastax.driver.core.LocalDate, Timestamp, UUID, String, String, Integer, Integer, String, BigDecimal, Integer, Boolean>> cassandraRows =
                matches
                        .map(e -> {
                            long reqMillis;
                            try {
                                reqMillis = Instant.parse(e.requestedAt).toEpochMilli();
                            } catch (Exception ex) {
                                reqMillis = System.currentTimeMillis();
                            }

                            com.datastax.driver.core.LocalDate dateBucket =
                                    com.datastax.driver.core.LocalDate.fromMillisSinceEpoch(reqMillis);
                            Timestamp createdAt = new Timestamp(reqMillis);
                            UUID tripId = UUID.fromString(e.tripId);

                            return Tuple12.of(
                                    e.city,
                                    dateBucket,
                                    createdAt,
                                    tripId,
                                    e.riderId,
                                    e.taxiId,
                                    e.originZone,
                                    e.destinationZone,
                                    e.status,
                                    e.fare,
                                    e.etaSeconds,
                                    e.matchedWithinSla
                            );
                        })
                        .returns(Types.TUPLE(
                                Types.STRING,
                                TypeInformation.of(com.datastax.driver.core.LocalDate.class),
                                Types.SQL_TIMESTAMP,
                                TypeInformation.of(UUID.class),
                                Types.STRING,
                                Types.STRING,
                                Types.INT,
                                Types.INT,
                                Types.STRING,
                                Types.BIG_DEC,
                                Types.INT,
                                Types.BOOLEAN
                        ))
                        .name("to-cassandra-rows");

        final String insertCql =
                "INSERT INTO taasim.trips (city, date_bucket, created_at, trip_id, rider_id, taxi_id, origin_zone, dest_zone, status, fare, eta_seconds, matched_within_sla) "
                        + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";

        CassandraSink.addSink(cassandraRows)
                .setQuery(insertCql)
                .setHost(cassandraHost, cassandraPort)
                .build();

        // Handle Side Output for Unmatched Requests
        final DataStream<TripRequestEvent> unmatchedTrips = matches.getSideOutput(TripMatcherFunction.unmatchedTag);

        final ObjectMapper mapper = new ObjectMapper();
        final DataStream<String> unmatchedJson = unmatchedTrips
                .map(mapper::writeValueAsString)
                .returns(Types.STRING)
                .name("unmatched-to-json");

        // Kafka Sink for Unmatched Requests
        final KafkaSink<String> unmatchedSink =
                KafkaSink.<String>builder()
                        .setBootstrapServers(kafkaBootstrap)
                        .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                        .setProperty("security.protocol", "SASL_PLAINTEXT")
                        .setProperty("sasl.mechanism", "PLAIN")
                        .setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"flink\" password=\"flink-secret\";")
                        .setRecordSerializer(
                                KafkaRecordSerializationSchema.<String>builder()
                                        .setTopic(unmatchedSinkTopic)
                                        .setValueSerializationSchema(new SimpleStringSchema())
                                        .build())
                        .build();

        unmatchedJson.sinkTo(unmatchedSink).name("kafka-raw.unmatched");
    }
}
