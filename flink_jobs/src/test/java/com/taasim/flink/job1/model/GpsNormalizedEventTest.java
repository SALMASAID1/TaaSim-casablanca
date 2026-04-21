package com.taasim.flink.job1.model;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class GpsNormalizedEventTest {

    @Test
    void toJson_containsContractFields() {
        final GpsNormalizedEvent e = new GpsNormalizedEvent();
        e.taxiId = "20000528";
        e.timestamp = "2026-04-20T14:30:12Z";
        e.lat = 33.6;
        e.lon = -7.6;
        e.speedKmh = 42.5f;
        e.status = "available";
        e.tripId = "";
        e.zoneId = 3;

        final String json = e.toJson();

        assertTrue(json.contains("\"taxi_id\""));
        assertTrue(json.contains("\"timestamp\""));
        assertTrue(json.contains("\"lat\""));
        assertTrue(json.contains("\"lon\""));
        assertTrue(json.contains("\"speed\""));
        assertTrue(json.contains("\"status\""));
        assertTrue(json.contains("\"trip_id\""));
        assertTrue(json.contains("\"arrondissement_id\""));
    }
}
