package com.taasim.flink.job2.model;

import java.io.Serializable;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

public class DemandZoneAggregate implements Serializable {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public String city;
    public int zoneId;
    public long windowStart;
    public int activeVehicles;
    public int pendingRequests;
    public float ratio;
    public float forecastDemand = 0.0f; // placeholder for ML forecasting

    public DemandZoneAggregate() {}

    public String toJson() {
        final ObjectNode node = MAPPER.createObjectNode();
        node.put("city", city);
        node.put("zoneId", zoneId);
        node.put("windowStart", windowStart);
        node.put("activeVehicles", activeVehicles);
        node.put("pendingRequests", pendingRequests);
        node.put("ratio", ratio);
        node.put("forecastDemand", forecastDemand);
        return node.toString();
    }
}
