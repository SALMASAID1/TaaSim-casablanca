package com.taasim.flink.job1.model;

import java.io.Serializable;

public class GpsRawEvent implements Serializable {
    public String taxiId;
    public String tripId;
    public String status;
    public String timestamp;
    public long eventTimeMillis;
    public double lat;
    public double lon;
    public float speedKmh;

    public GpsRawEvent() {}
}
