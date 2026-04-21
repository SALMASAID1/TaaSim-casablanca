package com.taasim.flink.job1.functions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.taasim.flink.job1.model.GpsRawEvent;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;

class ParseGpsEventFnTest {

    @Test
    void flatMap_parsesValidJsonAndExtractsEventTime() {
        final ParseGpsEventFn fn = new ParseGpsEventFn();

        final List<GpsRawEvent> out = new ArrayList<>();
        fn.flatMap(
                "{\"taxi_id\":\"20000528\",\"timestamp\":\"2026-04-20T14:30:12Z\",\"lat\":33.592312,\"lon\":-7.612903,\"speed\":42.5,\"status\":\"available\",\"trip_id\":\"\"}",
                new ListCollector<>(out));

        assertEquals(1, out.size());
        final GpsRawEvent e = out.get(0);
        assertEquals("20000528", e.taxiId);
        assertEquals("2026-04-20T14:30:12Z", e.timestamp);
        assertEquals(Instant.parse("2026-04-20T14:30:12Z").toEpochMilli(), e.eventTimeMillis);
        assertEquals(33.592312, e.lat, 1e-9);
        assertEquals(-7.612903, e.lon, 1e-9);
        assertEquals(42.5f, e.speedKmh, 1e-6);
        assertEquals("available", e.status);
        assertEquals("", e.tripId);
    }

    @Test
    void flatMap_dropsBadJson() {
        final ParseGpsEventFn fn = new ParseGpsEventFn();

        final List<GpsRawEvent> out = new ArrayList<>();
        fn.flatMap("not-json", new ListCollector<>(out));

        assertTrue(out.isEmpty());
    }

    private static final class ListCollector<T> implements Collector<T> {
        private final List<T> items;

        private ListCollector(List<T> items) {
            this.items = items;
        }

        @Override
        public void collect(T record) {
            items.add(record);
        }

        @Override
        public void close() {
            // no-op
        }
    }
}
