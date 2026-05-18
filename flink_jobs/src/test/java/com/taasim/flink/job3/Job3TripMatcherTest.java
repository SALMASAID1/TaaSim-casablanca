package com.taasim.flink.job3;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

class Job3TripMatcherTest {

    @Test
    void buildJob_buildsPipelineWithoutExecuting() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        final ParameterTool params =
                ParameterTool.fromArgs(
                        new String[] {
                            "--kafka-bootstrap-servers", "localhost:9092",
                            "--gps-source-topic", "processed.gps",
                            "--trips-source-topic", "raw.trips",
                            "--unmatched-sink-topic", "raw.unmatched",
                            "--cassandra-host", "localhost",
                            "--cassandra-port", "9042",
                            "--checkpoint-dir", "file:///tmp/taasim/flink-checkpoints/job3/",
                            "--checkpoint-interval-ms", "1000"
                        });

        assertDoesNotThrow(() -> Job3TripMatcher.buildJob(env, params));
    }
}
