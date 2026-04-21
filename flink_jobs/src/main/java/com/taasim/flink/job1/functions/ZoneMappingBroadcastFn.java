package com.taasim.flink.job1.functions;

import com.taasim.flink.job1.model.GpsNormalizedEvent;
import com.taasim.flink.job1.model.GpsRawEvent;
import com.taasim.flink.job1.model.ZoneDefinition;
import java.util.Map;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class ZoneMappingBroadcastFn
        extends BroadcastProcessFunction<GpsRawEvent, ZoneDefinition, GpsNormalizedEvent> {
    private final MapStateDescriptor<Integer, ZoneDefinition> zoneStateDescriptor;
    private final String city;

    private transient Counter zoneNotFound;

    public ZoneMappingBroadcastFn(
            MapStateDescriptor<Integer, ZoneDefinition> zoneStateDescriptor, String city) {
        this.zoneStateDescriptor = zoneStateDescriptor;
        this.city = city;
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) {
        this.zoneNotFound = getRuntimeContext().getMetricGroup().counter("zone_not_found");
    }

    @Override
    public void processBroadcastElement(ZoneDefinition value, Context ctx, Collector<GpsNormalizedEvent> out)
            throws Exception {
        if (value == null) {
            return;
        }
        ctx.getBroadcastState(zoneStateDescriptor).put(value.arrondissementId, value);
    }

    @Override
    public void processElement(GpsRawEvent value, ReadOnlyContext ctx, Collector<GpsNormalizedEvent> out)
            throws Exception {
        if (value == null) {
            return;
        }

        final ReadOnlyBroadcastState<Integer, ZoneDefinition> zones = ctx.getBroadcastState(zoneStateDescriptor);
        ZoneDefinition matched = null;
        for (Map.Entry<Integer, ZoneDefinition> entry : zones.immutableEntries()) {
            final ZoneDefinition zone = entry.getValue();
            if (zone != null && zone.contains(value.lon, value.lat)) {
                matched = zone;
                break;
            }
        }

        if (matched == null) {
            if (zoneNotFound != null) {
                zoneNotFound.inc();
            }
            return;
        }

        final GpsNormalizedEvent normalized = new GpsNormalizedEvent();
        normalized.city = city;
        normalized.zoneId = matched.arrondissementId;
        normalized.taxiId = value.taxiId;
        normalized.tripId = value.tripId;
        normalized.status = value.status;
        normalized.timestamp = value.timestamp;
        normalized.eventTimeMillis = value.eventTimeMillis;
        normalized.speedKmh = value.speedKmh;
        normalized.lat = matched.centroidLat;
        normalized.lon = matched.centroidLon;

        out.collect(normalized);
    }
}
