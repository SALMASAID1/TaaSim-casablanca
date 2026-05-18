package com.taasim.flink.job2.model;

import java.io.Serializable;
import java.time.Instant;

public class TripRequestEvent implements Serializable {
    public String tripId;
    public String riderId;
    public int originZone;
    public int destinationZone;
    public String requestedAt;
    public String callType;

    public TripRequestEvent() {}

    public long getRequestedAtMillis() {
        if (requestedAt == null || requestedAt.isBlank()) {
            return 0L;
        }
        try {
            return Instant.parse(requestedAt).toEpochMilli();
        } catch (Exception e) {
            return 0L;
        }
    }
}
