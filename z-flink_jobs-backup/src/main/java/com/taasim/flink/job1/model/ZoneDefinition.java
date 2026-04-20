package com.taasim.flink.job1.model;

import java.io.Serializable;

public class ZoneDefinition implements Serializable {
    public int arrondissementId;
    public String zoneName;
    public double lonMin;
    public double lonMax;
    public double latMin;
    public double latMax;
    public double centroidLon;
    public double centroidLat;

    public ZoneDefinition() {}

    public boolean contains(double lon, double lat) {
        return lon >= lonMin && lon <= lonMax && lat >= latMin && lat <= latMax;
    }
}
