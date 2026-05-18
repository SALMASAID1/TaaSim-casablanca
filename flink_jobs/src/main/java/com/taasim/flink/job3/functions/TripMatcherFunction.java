package com.taasim.flink.job3.functions;

import com.taasim.flink.job1.model.ZoneDefinition;
import com.taasim.flink.job1.util.ZoneMappingLoader;
import com.taasim.flink.job3.model.GpsProcessedEvent;
import com.taasim.flink.job3.model.TripMatchEvent;
import com.taasim.flink.job3.model.TripRequestEvent;
import com.taasim.flink.job3.model.VehicleInfo;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class TripMatcherFunction extends KeyedBroadcastProcessFunction<Integer, TripRequestEvent, GpsProcessedEvent, TripMatchEvent> {

    public static final OutputTag<TripRequestEvent> unmatchedTag =
            new OutputTag<>("unmatched-trips", TypeInformation.of(TripRequestEvent.class));

    private final MapStateDescriptor<String, VehicleInfo> vehicleStateDesc;
    private transient MapState<String, TripRequestEvent> pendingRequests;
    private transient ConcurrentHashMap<String, Long> localAssignedTaxis;
    private transient List<ZoneDefinition> zones;

    public TripMatcherFunction(MapStateDescriptor<String, VehicleInfo> vehicleStateDesc) {
        if (vehicleStateDesc == null) {
            throw new IllegalArgumentException("vehicleStateDesc must not be null");
        }
        this.vehicleStateDesc = vehicleStateDesc;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        this.pendingRequests = getRuntimeContext().getMapState(
                new MapStateDescriptor<>(
                        "pending-requests",
                        Types.STRING,
                        TypeInformation.of(TripRequestEvent.class)
                )
        );
        this.localAssignedTaxis = new ConcurrentHashMap<>();
        this.zones = ZoneMappingLoader.loadZonesFromClasspath();
    }

    @Override
    public void processElement(TripRequestEvent request, ReadOnlyContext ctx, Collector<TripMatchEvent> out) throws Exception {
        // Save the request to pending state
        pendingRequests.put(request.tripId, request);

        // First Search: Same Zone
        ReadOnlyBroadcastState<String, VehicleInfo> vehiclesState = ctx.getBroadcastState(vehicleStateDesc);
        VehicleInfo matchedVehicle = null;

        for (Map.Entry<String, VehicleInfo> entry : vehiclesState.immutableEntries()) {
            VehicleInfo vehicle = entry.getValue();
            if (vehicle.zoneId == request.originZone 
                    && "available".equalsIgnoreCase(vehicle.status)
                    && !isLocallyAssigned(vehicle.taxiId)) {
                if (matchedVehicle == null || vehicle.eventTimeMillis < matchedVehicle.eventTimeMillis) {
                    matchedVehicle = vehicle;
                }
            }
        }

        if (matchedVehicle != null) {
            // We found a match!
            markLocallyAssigned(matchedVehicle.taxiId);

            // Compute distance and ETA
            ZoneDefinition originZoneDef = getZoneDefinition(request.originZone);
            double latCentroid = originZoneDef != null ? originZoneDef.centroidLat : matchedVehicle.lat;
            double lonCentroid = originZoneDef != null ? originZoneDef.centroidLon : matchedVehicle.lon;
            double distanceKm = haversineDistance(matchedVehicle.lat, matchedVehicle.lon, latCentroid, lonCentroid);
            int etaSeconds = (int) Math.max(10.0, (distanceKm / 25.0) * 3600.0);

            TripMatchEvent match = new TripMatchEvent();
            match.city = "casablanca";
            match.tripId = request.tripId;
            match.riderId = request.riderId;
            match.taxiId = matchedVehicle.taxiId;
            match.originZone = request.originZone;
            match.destinationZone = request.destinationZone;
            match.requestedAt = request.requestedAt;
            match.status = "matched";
            match.fare = new BigDecimal("10.00");
            match.etaSeconds = etaSeconds;
            match.matchedWithinSla = true;

            out.collect(match);
            pendingRequests.remove(request.tripId);
        } else {
            // No vehicle immediately available, register event-time SLA timer for 5 seconds
            long timerTime = request.getRequestedAtMillis() + 5000L;
            ctx.timerService().registerEventTimeTimer(timerTime);
        }
    }

    @Override
    public void processBroadcastElement(GpsProcessedEvent gps, Context ctx, Collector<TripMatchEvent> out) throws Exception {
        BroadcastState<String, VehicleInfo> broadcastState = ctx.getBroadcastState(vehicleStateDesc);
        if ("offline".equalsIgnoreCase(gps.status)) {
            broadcastState.remove(gps.taxiId);
        } else {
            VehicleInfo info = new VehicleInfo(
                    gps.taxiId,
                    gps.zoneId,
                    gps.lat,
                    gps.lon,
                    gps.status,
                    gps.eventTimeMillis
            );
            broadcastState.put(gps.taxiId, info);
        }

        if (localAssignedTaxis != null) {
            localAssignedTaxis.remove(gps.taxiId);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<TripMatchEvent> out) throws Exception {
        // Collect all pending requests under this origin zone that have expired
        List<String> expiredTripIds = new ArrayList<>();
        for (Map.Entry<String, TripRequestEvent> entry : pendingRequests.entries()) {
            TripRequestEvent request = entry.getValue();
            long expirationTime = request.getRequestedAtMillis() + 5000L;
            if (timestamp >= expirationTime) {
                expiredTripIds.add(request.tripId);
            }
        }

        for (String tripId : expiredTripIds) {
            TripRequestEvent request = pendingRequests.get(tripId);
            if (request == null) {
                continue;
            }

            // Second Search: Adjacent Zone Fallback
            VehicleInfo matchedVehicle = findVehicleInAdjacentZones(request, ctx);

            if (matchedVehicle != null) {
                markLocallyAssigned(matchedVehicle.taxiId);

                // Recalculate ETA using actual distance from vehicle to request's origin zone centroid
                ZoneDefinition originZoneDef = getZoneDefinition(request.originZone);
                double latCentroid = originZoneDef != null ? originZoneDef.centroidLat : matchedVehicle.lat;
                double lonCentroid = originZoneDef != null ? originZoneDef.centroidLon : matchedVehicle.lon;
                double distanceKm = haversineDistance(matchedVehicle.lat, matchedVehicle.lon, latCentroid, lonCentroid);
                int etaSeconds = (int) Math.max(10.0, (distanceKm / 25.0) * 3600.0);

                TripMatchEvent match = new TripMatchEvent();
                match.city = "casablanca";
                match.tripId = request.tripId;
                match.riderId = request.riderId;
                match.taxiId = matchedVehicle.taxiId;
                match.originZone = request.originZone;
                match.destinationZone = request.destinationZone;
                match.requestedAt = request.requestedAt;
                match.status = "matched";
                match.fare = new BigDecimal("10.00");
                match.etaSeconds = etaSeconds;
                match.matchedWithinSla = false;

                out.collect(match);
            } else {
                // Still unmatched: emit to side output
                ctx.output(unmatchedTag, request);
            }
            pendingRequests.remove(tripId);
        }
    }

    private VehicleInfo findVehicleInAdjacentZones(TripRequestEvent request, OnTimerContext ctx) throws Exception {
        ReadOnlyBroadcastState<String, VehicleInfo> vehiclesState = ctx.getBroadcastState(vehicleStateDesc);
        List<ZoneDefinition> sortedZones = getZonesSortedByProximity(request.originZone);

        for (ZoneDefinition zone : sortedZones) {
            VehicleInfo matchedVehicle = null;
            for (Map.Entry<String, VehicleInfo> entry : vehiclesState.immutableEntries()) {
                VehicleInfo vehicle = entry.getValue();
                if (vehicle.zoneId == zone.arrondissementId 
                        && "available".equalsIgnoreCase(vehicle.status)
                        && !isLocallyAssigned(vehicle.taxiId)) {
                    if (matchedVehicle == null || vehicle.eventTimeMillis < matchedVehicle.eventTimeMillis) {
                        matchedVehicle = vehicle;
                    }
                }
            }
            if (matchedVehicle != null) {
                return matchedVehicle;
            }
        }
        return null;
    }

    private List<ZoneDefinition> getZonesSortedByProximity(int originZoneId) {
        ZoneDefinition originZone = getZoneDefinition(originZoneId);
        if (originZone == null || zones == null) {
            return Collections.emptyList();
        }
        List<ZoneDefinition> others = new ArrayList<>();
        for (ZoneDefinition zone : zones) {
            if (zone.arrondissementId != originZoneId) {
                others.add(zone);
            }
        }
        others.sort((z1, z2) -> {
            double d1 = haversineDistance(originZone.centroidLat, originZone.centroidLon, z1.centroidLat, z1.centroidLon);
            double d2 = haversineDistance(originZone.centroidLat, originZone.centroidLon, z2.centroidLat, z2.centroidLon);
            return Double.compare(d1, d2);
        });
        return others;
    }

    private ZoneDefinition getZoneDefinition(int zoneId) {
        if (zones == null) {
            return null;
        }
        for (ZoneDefinition zone : zones) {
            if (zone.arrondissementId == zoneId) {
                return zone;
            }
        }
        return null;
    }

    public static double haversineDistance(double lat1, double lon1, double lat2, double lon2) {
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);
        double a = Math.sin(dLat / 2.0) * Math.sin(dLat / 2.0)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                * Math.sin(dLon / 2.0) * Math.sin(dLon / 2.0);
        return 2.0 * 6371.0 * Math.asin(Math.sqrt(a));
    }

    private boolean isLocallyAssigned(String taxiId) {
        if (localAssignedTaxis == null) {
            return false;
        }
        Long expiry = localAssignedTaxis.get(taxiId);
        if (expiry == null) {
            return false;
        }
        if (System.currentTimeMillis() > expiry) {
            localAssignedTaxis.remove(taxiId);
            return false;
        }
        return true;
    }

    private void markLocallyAssigned(String taxiId) {
        if (localAssignedTaxis != null) {
            localAssignedTaxis.put(taxiId, System.currentTimeMillis() + 10_000L);
        }
    }
}
