package com.taasim.flink.job1.functions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.taasim.flink.job1.model.GpsRawEvent;
import java.lang.reflect.Field;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.metrics.Counter;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;

class ParseGpsEventFnTest {

    @Test
    void nullOrBlankInput_incrementsBadEventsAndEmitsNothing() throws Exception {
        final ParseGpsEventFn fn = new ParseGpsEventFn();
        final TestCounter parseErrors = new TestCounter();
        final TestCounter badEvents = new TestCounter();
        setCounter(fn, "parseErrors", parseErrors);
        setCounter(fn, "badEvents", badEvents);

        final ListCollector<GpsRawEvent> out = new ListCollector<>();

        fn.flatMap(null, out);
        fn.flatMap("   ", out);

        assertEquals(0L, parseErrors.getCount());
        assertEquals(2L, badEvents.getCount());
        assertTrue(out.values().isEmpty());
    }

    @Test
    void invalidJson_incrementsParseErrorsAndEmitsNothing() throws Exception {
        final ParseGpsEventFn fn = new ParseGpsEventFn();
        final TestCounter parseErrors = new TestCounter();
        final TestCounter badEvents = new TestCounter();
        setCounter(fn, "parseErrors", parseErrors);
        setCounter(fn, "badEvents", badEvents);

        final ListCollector<GpsRawEvent> out = new ListCollector<>();

        fn.flatMap("{not-json}", out);

        assertEquals(1L, parseErrors.getCount());
        assertEquals(0L, badEvents.getCount());
        assertTrue(out.values().isEmpty());
    }

    @Test
    void missingRequiredFields_incrementsBadEventsAndEmitsNothing() throws Exception {
        final ParseGpsEventFn fn = new ParseGpsEventFn();
        final TestCounter parseErrors = new TestCounter();
        final TestCounter badEvents = new TestCounter();
        setCounter(fn, "parseErrors", parseErrors);
        setCounter(fn, "badEvents", badEvents);

        final ListCollector<GpsRawEvent> out = new ListCollector<>();

        // Missing lat/lon => NaN defaults => treated as bad event
        fn.flatMap("{\"taxi_id\":\"t1\",\"timestamp\":\"2026-04-19T20:00:00Z\"}", out);

        assertEquals(0L, parseErrors.getCount());
        assertEquals(1L, badEvents.getCount());
        assertTrue(out.values().isEmpty());
    }

    @Test
    void validJson_emitsParsedEvent() throws Exception {
        final ParseGpsEventFn fn = new ParseGpsEventFn();
        final TestCounter parseErrors = new TestCounter();
        final TestCounter badEvents = new TestCounter();
        setCounter(fn, "parseErrors", parseErrors);
        setCounter(fn, "badEvents", badEvents);

        final ListCollector<GpsRawEvent> out = new ListCollector<>();

        final String ts = "2026-04-19T20:00:00Z";
        final String json =
                "{"
                + "\"taxi_id\":\" taxi-1 \","
                + "\"timestamp\":\""
                + ts
                + "\" ,"
                        + "\"lat\":33.6,"
                        + "\"lon\":-7.5,"
                        + "\"speed\":12.3,"
                        + "\"status\":\"FREE\","
                        + "\"trip_id\":\"trip-9\""
                        + "}";

        fn.flatMap(json, out);

        assertEquals(0L, parseErrors.getCount());
        assertEquals(0L, badEvents.getCount());
        assertEquals(1, out.values().size());

        final GpsRawEvent e = out.values().get(0);
        assertEquals("taxi-1", e.taxiId);
        assertEquals(ts, e.timestamp);
        assertEquals(33.6, e.lat, 1e-9);
        assertEquals(-7.5, e.lon, 1e-9);
        assertEquals(12.3f, e.speedKmh, 1e-6);
        assertEquals("FREE", e.status);
        assertEquals("trip-9", e.tripId);
        assertEquals(Instant.parse(ts).toEpochMilli(), e.eventTimeMillis);
    }

    private static void setCounter(ParseGpsEventFn fn, String fieldName, Counter counter)
            throws Exception {
        final Field field = ParseGpsEventFn.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(fn, counter);
    }

    private static final class TestCounter implements Counter {
        private long count;

        @Override
        public void inc() {
            count++;
        }

        @Override
        public void inc(long n) {
            count += n;
        }

        @Override
        public void dec() {
            count--;
        }

        @Override
        public void dec(long n) {
            count -= n;
        }

        @Override
        public long getCount() {
            return count;
        }
    }

    private static final class ListCollector<T> implements Collector<T> {
        private final List<T> values = new ArrayList<>();

        @Override
        public void collect(T record) {
            values.add(record);
        }

        @Override
        public void close() {
            // no-op
        }

        public List<T> values() {
            return values;
        }
    }
}
