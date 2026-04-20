package com.taasim.flink.job1.model;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Instant;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

class GpsNormalizedEventTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void toJson_usesTimestampWhenPresent() throws Exception {
        final GpsNormalizedEvent e = new GpsNormalizedEvent();
        e.taxiId = "taxi-1";
        e.tripId = "trip-9";
        e.status = "FREE";
        e.timestamp = "2026-04-19T20:00:00Z";
        e.eventTimeMillis = 0L;
        e.lat = 33.6;
        e.lon = -7.5;
        e.speedKmh = 12.5f;
        e.zoneId = 3;

        final JsonNode node = MAPPER.readTree(e.toJson());
        assertEquals("taxi-1", node.get("taxi_id").asText());
        assertEquals(e.timestamp, node.get("timestamp").asText());
        assertEquals(33.6, node.get("lat").asDouble(), 1e-9);
        assertEquals(-7.5, node.get("lon").asDouble(), 1e-9);
        assertEquals(12.5, node.get("speed").asDouble(), 1e-6);
        assertEquals("FREE", node.get("status").asText());
        assertEquals("trip-9", node.get("trip_id").asText());
        assertEquals(3, node.get("arrondissement_id").asInt());
    }

    @Test
    void toJson_fallsBackToEventTimeWhenTimestampBlank() throws Exception {
        final GpsNormalizedEvent e = new GpsNormalizedEvent();
        e.taxiId = "taxi-1";
        e.timestamp = " ";
        e.eventTimeMillis = 0L;
        e.lat = 33.6;
        e.lon = -7.5;
        e.speedKmh = 0.0f;
        e.zoneId = 0;

        final JsonNode node = MAPPER.readTree(e.toJson());
        assertEquals(Instant.ofEpochMilli(0).toString(), node.get("timestamp").asText());
    }
}
