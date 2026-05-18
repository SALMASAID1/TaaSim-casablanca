package com.taasim.flink.job2;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

class Job2DemandAggregatorTest {

    @Test
    void buildJob_buildsPipelineWithoutExecuting() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        final ParameterTool params =
                ParameterTool.fromArgs(
                        new String[] {
                            "--kafka-bootstrap-servers", "localhost:9092",
                            "--gps-source-topic", "processed.gps",
                            "--trips-source-topic", "raw.trips",
                            "--demand-sink-topic", "processed.demand",
                            "--cassandra-host", "localhost",
                            "--cassandra-port", "9042",
                            "--checkpoint-dir", "file:///tmp/taasim/flink-checkpoints/job2/",
                            "--checkpoint-interval-ms", "1000"
                        });

        assertDoesNotThrow(() -> Job2DemandAggregator.buildJob(env, params));
    }
}
