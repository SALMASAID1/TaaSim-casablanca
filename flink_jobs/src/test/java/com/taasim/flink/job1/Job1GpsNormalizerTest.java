package com.taasim.flink.job1;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

class Job1GpsNormalizerTest {

    @Test
    void buildJob_buildsPipelineWithoutExecuting() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        final ParameterTool params =
                ParameterTool.fromArgs(
                        new String[] {
                            "--kafka-bootstrap-servers", "localhost:9092",
                            "--source-topic", "raw.gps",
                            "--sink-topic", "processed.gps",
                            "--cassandra-host", "localhost",
                            "--cassandra-port", "9042",
                            "--city", "casablanca",
                            "--checkpoint-dir", "file:///tmp/taasim/flink-checkpoints/job1/",
                            "--checkpoint-interval-ms", "1000"
                        });

        assertDoesNotThrow(() -> Job1GpsNormalizer.buildJob(env, params));
    }
}
