package com.taasim.flink.job1.functions;

import com.taasim.flink.job1.model.GpsRawEvent;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ValidationAndLateFilterFn extends ProcessFunction<GpsRawEvent, GpsRawEvent> {
    private static final Logger LOG = LoggerFactory.getLogger(ValidationAndLateFilterFn.class);

    // Casablanca bbox (lon: 7.4°W–7.8°W, lat: 33.4°N–33.7°N)
    private static final double LON_MIN = -7.8;
    private static final double LON_MAX = -7.4;
    private static final double LAT_MIN = 33.4;
    private static final double LAT_MAX = 33.7;
    private static final float MAX_SPEED_KMH = 150.0f;

    public static final OutputTag<GpsRawEvent> LATE_EVENTS_TAG = new OutputTag<GpsRawEvent>("late_events") {};

    private transient Counter invalidBbox;
    private transient Counter speedTooHigh;
    private transient Counter droppedLate;

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) {
        this.invalidBbox = getRuntimeContext().getMetricGroup().counter("invalid_bbox");
        this.speedTooHigh = getRuntimeContext().getMetricGroup().counter("speed_too_high");
        this.droppedLate = getRuntimeContext().getMetricGroup().counter("dropped_late");
    }

    @Override
    public void processElement(GpsRawEvent value, Context ctx, Collector<GpsRawEvent> out) {
        if (value == null) {
            return;
        }

        // Speed validation
        if (value.speedKmh > MAX_SPEED_KMH) {
            speedTooHigh.inc();
            return;
        }

        // Casablanca bbox validation (raw coords)
        if (value.lon < LON_MIN || value.lon > LON_MAX || value.lat < LAT_MIN || value.lat > LAT_MAX) {
            invalidBbox.inc();
            return;
        }

        // Late event detection: beyond current watermark => drop and side-output
        final long watermark = ctx.timerService().currentWatermark();
        if (watermark != Long.MIN_VALUE && value.eventTimeMillis < watermark) {
            droppedLate.inc();
            ctx.output(LATE_EVENTS_TAG, value);
            LOG.info(
                    "Dropped late event (ts={} < watermark={}) taxi_id={}",
                    value.timestamp,
                    watermark,
                    value.taxiId);
            return;
        }

        out.collect(value);
    }
}
