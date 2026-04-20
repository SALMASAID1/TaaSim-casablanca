package com.taasim.flink.job1.model;

import java.io.Serializable;
import java.time.Instant;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

public class GpsNormalizedEvent implements Serializable {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public String city;
    public int zoneId;
    public String taxiId;
    public String tripId;
    public String status;
    public String timestamp;
    public long eventTimeMillis;
    public double lat;
    public double lon;
    public float speedKmh;

    public GpsNormalizedEvent() {}

    public String toJson() {
        final ObjectNode node = MAPPER.createObjectNode();
        node.put("taxi_id", taxiId);
        node.put("timestamp", timestamp != null && !timestamp.isBlank() ? timestamp : Instant.ofEpochMilli(eventTimeMillis).toString());
        node.put("lat", lat);
        node.put("lon", lon);
        node.put("speed", speedKmh);
        node.put("status", status);
        node.put("trip_id", tripId);
        node.put("arrondissement_id", zoneId);
        return node.toString();
    }
}
