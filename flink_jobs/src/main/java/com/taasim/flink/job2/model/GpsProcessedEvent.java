package com.taasim.flink.job2.model;

import java.io.Serializable;

public class GpsProcessedEvent implements Serializable {
    public String city;
    public int zoneId;
    public long eventTimeMillis;
    public String taxiId;
    public double lat;
    public double lon;
    public float speedKmh;
    public String status;

    public GpsProcessedEvent() {}
}
