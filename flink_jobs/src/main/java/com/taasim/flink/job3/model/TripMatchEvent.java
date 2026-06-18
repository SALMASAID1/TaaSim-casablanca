package com.taasim.flink.job3.model;

import java.io.Serializable;
import java.math.BigDecimal;

public class TripMatchEvent implements Serializable {
    public String city;
    public String tripId;
    public String riderId;
    public String taxiId;
    public int originZone;
    public int destinationZone;
    public String requestedAt;
    public String status;
    public BigDecimal fare;
    public int etaSeconds;
    public boolean matchedWithinSla;

    public TripMatchEvent() {}
}
