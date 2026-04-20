package com.taasim.flink.job1.functions;

import com.taasim.flink.job1.model.GpsRawEvent;
import java.time.Instant;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.metrics.Counter;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.Collector;

public class ParseGpsEventFn extends RichFlatMapFunction<String, GpsRawEvent> {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private transient Counter parseErrors;
    private transient Counter badEvents;

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) {
        this.parseErrors = getRuntimeContext().getMetricGroup().counter("parse_errors");
        this.badEvents = getRuntimeContext().getMetricGroup().counter("bad_events");
    }

    @Override
    public void flatMap(String value, Collector<GpsRawEvent> out) {
        if (value == null || value.isBlank()) {
            badEvents.inc();
            return;
        }

        try {
            final JsonNode node = MAPPER.readTree(value);

            final String taxiId = node.path("taxi_id").asText("").trim();
            final String timestamp = node.path("timestamp").asText("").trim();
            final double lat = node.path("lat").asDouble(Double.NaN);
            final double lon = node.path("lon").asDouble(Double.NaN);
            final float speed = (float) node.path("speed").asDouble(0.0);
            final String status = node.path("status").asText("");
            final String tripId = node.path("trip_id").asText("");

            if (taxiId.isEmpty() || timestamp.isEmpty() || Double.isNaN(lat) || Double.isNaN(lon)) {
                badEvents.inc();
                return;
            }

            final long eventTimeMillis = Instant.parse(timestamp).toEpochMilli();

            final GpsRawEvent e = new GpsRawEvent();
            e.taxiId = taxiId;
            e.timestamp = timestamp;
            e.eventTimeMillis = eventTimeMillis;
            e.lat = lat;
            e.lon = lon;
            e.speedKmh = speed;
            e.status = status;
            e.tripId = tripId;

            out.collect(e);
        } catch (Exception ex) {
            parseErrors.inc();
        }
    }
}
