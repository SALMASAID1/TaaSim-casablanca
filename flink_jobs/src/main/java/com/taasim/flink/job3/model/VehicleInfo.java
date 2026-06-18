package com.taasim.flink.job3.model;

import java.io.Serializable;

public class VehicleInfo implements Serializable {
    public String taxiId;
    public int zoneId;
    public double lat;
    public double lon;
    public String status;
    public long eventTimeMillis;

    public VehicleInfo() {}

    public VehicleInfo(String taxiId, int zoneId, double lat, double lon, String status, long eventTimeMillis) {
        this.taxiId = taxiId;
        this.zoneId = zoneId;
        this.lat = lat;
        this.lon = lon;
        this.status = status;
        this.eventTimeMillis = eventTimeMillis;
    }
}
