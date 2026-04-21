package com.taasim.flink.job1.functions;

import com.taasim.flink.job1.model.GpsRawEvent;
import java.time.Instant;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.metrics.Counter;
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
            if (badEvents != null) {
                badEvents.inc();
            }
            return;
        }

        final JsonNode node;
        try {
            node = MAPPER.readTree(value);
        } catch (JsonProcessingException e) {
            if (parseErrors != null) {
                parseErrors.inc();
            }
            return;
        }

        final String taxiId = textOrNull(node.get("taxi_id"));
        final String timestamp = textOrNull(node.get("timestamp"));
        final Double lat = doubleOrNull(node.get("lat"));
        final Double lon = doubleOrNull(node.get("lon"));
        final Float speedKmh = floatOrNull(node.get("speed"));
        final String status = textOrNull(node.get("status"));
        final String tripId = node.has("trip_id") && !node.get("trip_id").isNull() ? node.get("trip_id").asText("") : "";

        if (taxiId == null || timestamp == null || lat == null || lon == null || speedKmh == null || status == null) {
            if (badEvents != null) {
                badEvents.inc();
            }
            return;
        }

        final long eventTimeMillis;
        try {
            eventTimeMillis = Instant.parse(timestamp).toEpochMilli();
        } catch (Exception e) {
            if (parseErrors != null) {
                parseErrors.inc();
            }
            return;
        }

        final GpsRawEvent event = new GpsRawEvent();
        event.taxiId = taxiId;
        event.timestamp = timestamp;
        event.eventTimeMillis = eventTimeMillis;
        event.lat = lat;
        event.lon = lon;
        event.speedKmh = speedKmh;
        event.status = status;
        event.tripId = tripId;

        out.collect(event);
    }

    private static String textOrNull(JsonNode node) {
        if (node == null || node.isNull()) {
            return null;
        }
        final String value = node.asText(null);
        if (value == null) {
            return null;
        }
        final String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    private static Double doubleOrNull(JsonNode node) {
        if (node == null || node.isNull()) {
            return null;
        }
        if (node.isNumber()) {
            return node.asDouble();
        }
        if (node.isTextual()) {
            final String raw = node.asText();
            try {
                return Double.parseDouble(raw);
            } catch (NumberFormatException e) {
                return null;
            }
        }
        return null;
    }

    private static Float floatOrNull(JsonNode node) {
        final Double value = doubleOrNull(node);
        return value == null ? null : value.floatValue();
    }
}
