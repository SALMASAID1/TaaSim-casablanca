package com.taasim.flink.job1.functions;

import com.taasim.flink.job1.model.GpsRawEvent;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class ValidationAndLateFilterFn extends ProcessFunction<GpsRawEvent, GpsRawEvent> {
    public static final OutputTag<GpsRawEvent> LATE_EVENTS_TAG = new OutputTag<GpsRawEvent>("late_events") {};

    private static final double CASABLANCA_LON_MIN = -7.8;
    private static final double CASABLANCA_LON_MAX = -7.4;
    private static final double CASABLANCA_LAT_MIN = 33.4;
    private static final double CASABLANCA_LAT_MAX = 33.7;
    private static final float MAX_SPEED_KMH = 150.0f;

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

        if (!isInCasablancaBbox(value.lon, value.lat)) {
            if (invalidBbox != null) {
                invalidBbox.inc();
            }
            return;
        }

        if (!isSpeedValid(value.speedKmh)) {
            if (speedTooHigh != null) {
                speedTooHigh.inc();
            }
            return;
        }

        final long currentWatermark = ctx.timerService().currentWatermark();
        if (isLate(value.eventTimeMillis, currentWatermark)) {
            if (droppedLate != null) {
                droppedLate.inc();
            }
            ctx.output(LATE_EVENTS_TAG, value);
            return;
        }

        out.collect(value);
    }

    static boolean isInCasablancaBbox(double lon, double lat) {
        return lon >= CASABLANCA_LON_MIN
                && lon <= CASABLANCA_LON_MAX
                && lat >= CASABLANCA_LAT_MIN
                && lat <= CASABLANCA_LAT_MAX;
    }

    static boolean isSpeedValid(float speedKmh) {
        return speedKmh <= MAX_SPEED_KMH;
    }

    static boolean isLate(long eventTimeMillis, long currentWatermark) {
        return currentWatermark != Long.MIN_VALUE && eventTimeMillis < currentWatermark;
    }
}
