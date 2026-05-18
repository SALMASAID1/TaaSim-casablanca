package com.taasim.flink.job3.functions;

import com.taasim.flink.job3.model.TripRequestEvent;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.metrics.Counter;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.Collector;

public class ParseTripRequestEventFn extends RichFlatMapFunction<String, TripRequestEvent> {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private transient Counter parseErrors;
    private transient Counter badEvents;

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) {
        this.parseErrors = getRuntimeContext().getMetricGroup().counter("parse_errors");
        this.badEvents = getRuntimeContext().getMetricGroup().counter("bad_events");
    }

    @Override
    public void flatMap(String value, Collector<TripRequestEvent> out) {
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

        final String tripId = textOrNull(node.get("tripId")) != null ? textOrNull(node.get("tripId")) :
                             textOrNull(node.get("trip_id"));
        
        final String riderId = textOrNull(node.get("riderId")) != null ? textOrNull(node.get("riderId")) :
                               textOrNull(node.get("rider_id"));

        final Integer originZone = intOrNull(node.get("originZone")) != null ? intOrNull(node.get("originZone")) :
                                   intOrNull(node.get("origin_zone"));

        final Integer destinationZone = intOrNull(node.get("destinationZone")) != null ? intOrNull(node.get("destinationZone")) :
                                        intOrNull(node.get("destination_zone"));

        final String requestedAt = textOrNull(node.get("requestedAt")) != null ? textOrNull(node.get("requestedAt")) :
                                   textOrNull(node.get("requested_at"));

        final String callType = textOrNull(node.get("callType")) != null ? textOrNull(node.get("callType")) :
                                textOrNull(node.get("call_type"));

        if (tripId == null || riderId == null || originZone == null || destinationZone == null || requestedAt == null || callType == null) {
            if (badEvents != null) {
                badEvents.inc();
            }
            return;
        }

        final TripRequestEvent event = new TripRequestEvent();
        event.tripId = tripId;
        event.riderId = riderId;
        event.originZone = originZone;
        event.destinationZone = destinationZone;
        event.requestedAt = requestedAt;
        event.callType = callType;

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
}
