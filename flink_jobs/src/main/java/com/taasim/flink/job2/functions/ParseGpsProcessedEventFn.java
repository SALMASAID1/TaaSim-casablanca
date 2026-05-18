package com.taasim.flink.job2.functions;

import com.taasim.flink.job2.model.GpsProcessedEvent;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.metrics.Counter;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.Collector;

public class ParseGpsProcessedEventFn extends RichFlatMapFunction<String, GpsProcessedEvent> {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private transient Counter parseErrors;
    private transient Counter badEvents;

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) {
        this.parseErrors = getRuntimeContext().getMetricGroup().counter("parse_errors");
        this.badEvents = getRuntimeContext().getMetricGroup().counter("bad_events");
    }

    @Override
    public void flatMap(String value, Collector<GpsProcessedEvent> out) {
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

        final String city = textOrNull(node.get("city")) != null ? textOrNull(node.get("city")) : "casablanca";
        
        // Support both zoneId and zone_id or arrondissement_id
        final Integer zoneId = intOrNull(node.get("zoneId")) != null ? intOrNull(node.get("zoneId")) :
                               (intOrNull(node.get("zone_id")) != null ? intOrNull(node.get("zone_id")) :
                                intOrNull(node.get("arrondissement_id")));
        
        Long eventTimeMillis = longOrNull(node.get("eventTimeMillis")) != null ? longOrNull(node.get("eventTimeMillis")) :
                               longOrNull(node.get("event_time_millis"));
        if (eventTimeMillis == null) {
            final String tsStr = textOrNull(node.get("timestamp"));
            if (tsStr != null) {
                try {
                    eventTimeMillis = java.time.Instant.parse(tsStr).toEpochMilli();
                } catch (Exception e) {
                    // Ignore and fallback
                }
            }
        }
        
        final String taxiId = textOrNull(node.get("taxiId")) != null ? textOrNull(node.get("taxiId")) :
                              textOrNull(node.get("taxi_id"));
        
        final Double lat = doubleOrNull(node.get("lat"));
        final Double lon = doubleOrNull(node.get("lon"));
        
        final Float speedKmh = floatOrNull(node.get("speedKmh")) != null ? floatOrNull(node.get("speedKmh")) :
                               (floatOrNull(node.get("speed_kmh")) != null ? floatOrNull(node.get("speed_kmh")) :
                                floatOrNull(node.get("speed")));
        
        final String status = textOrNull(node.get("status"));

        if (city == null || zoneId == null || eventTimeMillis == null || taxiId == null || lat == null || lon == null || speedKmh == null || status == null) {
            if (badEvents != null) {
                badEvents.inc();
            }
            return;
        }

        final GpsProcessedEvent event = new GpsProcessedEvent();
        event.city = city;
        event.zoneId = zoneId;
        event.eventTimeMillis = eventTimeMillis;
        event.taxiId = taxiId;
        event.lat = lat;
        event.lon = lon;
        event.speedKmh = speedKmh;
        event.status = status;

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

    private static Integer intOrNull(JsonNode node) {
        if (node == null || node.isNull()) {
            return null;
        }
        if (node.isNumber()) {
            return node.asInt();
        }
        if (node.isTextual()) {
            try {
                return Integer.parseInt(node.asText());
            } catch (NumberFormatException e) {
                return null;
            }
        }
        return null;
    }

    private static Long longOrNull(JsonNode node) {
        if (node == null || node.isNull()) {
            return null;
        }
        if (node.isNumber()) {
            return node.asLong();
        }
        if (node.isTextual()) {
            try {
                return Long.parseLong(node.asText());
            } catch (NumberFormatException e) {
                return null;
            }
        }
        return null;
    }

    private static Double doubleOrNull(JsonNode node) {
        if (node == null || node.isNull()) {
            return null;
        }
        if (node.isNumber()) {
            return node.asDouble();
        }
        if (node.isTextual()) {
            try {
                return Double.parseDouble(node.asText());
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
