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

    private transient Counter unmappedZone;

    public ZoneMappingBroadcastFn(
            MapStateDescriptor<Integer, ZoneDefinition> zoneStateDescriptor, String city) {
        this.zoneStateDescriptor = zoneStateDescriptor;
        this.city = city;
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) {
        this.unmappedZone = getRuntimeContext().getMetricGroup().counter("zone_not_found");
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
            final ZoneDefinition z = entry.getValue();
            if (z != null && z.contains(value.lon, value.lat)) {
                matched = z;
                break;
            }
        }

        if (matched == null) {
            unmappedZone.inc();
            return;
        }

        final GpsNormalizedEvent e = new GpsNormalizedEvent();
        e.city = city;
        e.zoneId = matched.arrondissementId;
        e.taxiId = value.taxiId;
        e.tripId = value.tripId;
        e.status = value.status;
        e.speedKmh = value.speedKmh;
        e.timestamp = value.timestamp;
        e.eventTimeMillis = value.eventTimeMillis;

        // Anonymisation: replace raw lat/lon with zone centroid
        e.lat = matched.centroidLat;
        e.lon = matched.centroidLon;

        out.collect(e);
    }
}
