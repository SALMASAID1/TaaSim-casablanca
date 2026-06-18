package com.taasim.flink.job2.model;

import java.io.Serializable;

public class UnifiedWindowInput implements Serializable {
    public String city;
    public int zoneId;
    public String eventType; // "VEHICLE" or "REQUEST"
    public String entityId;  // taxiId or tripId
    public long eventTimeMillis;

    public UnifiedWindowInput() {}
}
