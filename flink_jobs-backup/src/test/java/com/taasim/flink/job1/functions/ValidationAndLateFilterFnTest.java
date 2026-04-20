package com.taasim.flink.job1.functions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.taasim.flink.job1.model.GpsRawEvent;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.junit.jupiter.api.Test;

class ValidationAndLateFilterFnTest {

    @Test
    void speedTooHigh_isDropped() throws Exception {
        final ValidationAndLateFilterFn fn = new ValidationAndLateFilterFn();
        final TestCounter invalidBbox = new TestCounter();
        final TestCounter speedTooHigh = new TestCounter();
        final TestCounter droppedLate = new TestCounter();
        setCounter(fn, "invalidBbox", invalidBbox);
        setCounter(fn, "speedTooHigh", speedTooHigh);
        setCounter(fn, "droppedLate", droppedLate);

        final SideOutputs sideOutputs = new SideOutputs();
        final ProcessFunction<GpsRawEvent, GpsRawEvent>.Context ctx =
                contextWithWatermark(fn, Long.MIN_VALUE, sideOutputs);
        final ListCollector<GpsRawEvent> out = new ListCollector<>();

        final GpsRawEvent e = validEvent();
        e.speedKmh = 200.0f;

        fn.processElement(e, ctx, out);

        assertEquals(0L, invalidBbox.getCount());
        assertEquals(1L, speedTooHigh.getCount());
        assertEquals(0L, droppedLate.getCount());
        assertTrue(out.values().isEmpty());
        assertTrue(sideOutputs.values().isEmpty());
    }

    @Test
    void outsideCasablancaBbox_isDropped() throws Exception {
        final ValidationAndLateFilterFn fn = new ValidationAndLateFilterFn();
        final TestCounter invalidBbox = new TestCounter();
        final TestCounter speedTooHigh = new TestCounter();
        final TestCounter droppedLate = new TestCounter();
        setCounter(fn, "invalidBbox", invalidBbox);
        setCounter(fn, "speedTooHigh", speedTooHigh);
        setCounter(fn, "droppedLate", droppedLate);

        final SideOutputs sideOutputs = new SideOutputs();
        final ProcessFunction<GpsRawEvent, GpsRawEvent>.Context ctx =
                contextWithWatermark(fn, Long.MIN_VALUE, sideOutputs);
        final ListCollector<GpsRawEvent> out = new ListCollector<>();

        final GpsRawEvent e = validEvent();
        e.lon = -7.95;

        fn.processElement(e, ctx, out);

        assertEquals(1L, invalidBbox.getCount());
        assertEquals(0L, speedTooHigh.getCount());
        assertEquals(0L, droppedLate.getCount());
        assertTrue(out.values().isEmpty());
        assertTrue(sideOutputs.values().isEmpty());
    }

    @Test
    void lateEvent_isSideOutputAndDropped() throws Exception {
        final ValidationAndLateFilterFn fn = new ValidationAndLateFilterFn();
        final TestCounter invalidBbox = new TestCounter();
        final TestCounter speedTooHigh = new TestCounter();
        final TestCounter droppedLate = new TestCounter();
        setCounter(fn, "invalidBbox", invalidBbox);
        setCounter(fn, "speedTooHigh", speedTooHigh);
        setCounter(fn, "droppedLate", droppedLate);

        final SideOutputs sideOutputs = new SideOutputs();
        final ProcessFunction<GpsRawEvent, GpsRawEvent>.Context ctx = contextWithWatermark(fn, 1000L, sideOutputs);
        final ListCollector<GpsRawEvent> out = new ListCollector<>();

        final GpsRawEvent e = validEvent();
        e.eventTimeMillis = 999L;

        fn.processElement(e, ctx, out);

        assertEquals(0L, invalidBbox.getCount());
        assertEquals(0L, speedTooHigh.getCount());
        assertEquals(1L, droppedLate.getCount());
        assertTrue(out.values().isEmpty());
        assertEquals(1, sideOutputs.values().size());
        assertEquals(e, sideOutputs.values().get(0));
    }

    @Test
    void onTimeEvent_isCollected() throws Exception {
        final ValidationAndLateFilterFn fn = new ValidationAndLateFilterFn();
        final TestCounter invalidBbox = new TestCounter();
        final TestCounter speedTooHigh = new TestCounter();
        final TestCounter droppedLate = new TestCounter();
        setCounter(fn, "invalidBbox", invalidBbox);
        setCounter(fn, "speedTooHigh", speedTooHigh);
        setCounter(fn, "droppedLate", droppedLate);

        final SideOutputs sideOutputs = new SideOutputs();
        final ProcessFunction<GpsRawEvent, GpsRawEvent>.Context ctx = contextWithWatermark(fn, 1000L, sideOutputs);
        final ListCollector<GpsRawEvent> out = new ListCollector<>();

        final GpsRawEvent e = validEvent();
        e.eventTimeMillis = 1000L;

        fn.processElement(e, ctx, out);

        assertEquals(0L, invalidBbox.getCount());
        assertEquals(0L, speedTooHigh.getCount());
        assertEquals(0L, droppedLate.getCount());
        assertEquals(1, out.values().size());
        assertEquals(e, out.values().get(0));
        assertTrue(sideOutputs.values().isEmpty());
    }

    private static ProcessFunction<GpsRawEvent, GpsRawEvent>.Context contextWithWatermark(
            ValidationAndLateFilterFn fn, long watermark, SideOutputs sideOutputs) {
        final TimerService timerService = new TestTimerService(watermark);

        return fn.new Context() {
            @Override
            public Long timestamp() {
                return null;
            }

            @Override
            public TimerService timerService() {
                return timerService;
            }

            @Override
            public <X> void output(OutputTag<X> outputTag, X value) {
                sideOutputs.output(outputTag, value);
            }
        };
    }

    private static void setCounter(ValidationAndLateFilterFn fn, String fieldName, Counter counter)
            throws Exception {
        final Field field = ValidationAndLateFilterFn.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(fn, counter);
    }

    private static GpsRawEvent validEvent() {
        final GpsRawEvent e = new GpsRawEvent();
        e.taxiId = "taxi-1";
        e.timestamp = "2026-04-19T20:00:00Z";
        e.eventTimeMillis = 1_000L;
        e.lat = 33.6;
        e.lon = -7.5;
        e.speedKmh = 10.0f;
        e.status = "FREE";
        e.tripId = "trip-9";
        return e;
    }

    private static final class SideOutputs {
        private final List<Object> values = new ArrayList<>();

        public <T> void output(OutputTag<T> tag, T value) {
            if (tag == ValidationAndLateFilterFn.LATE_EVENTS_TAG) {
                values.add(value);
            }
        }

        public List<Object> values() {
            return values;
        }
    }

    private static final class TestTimerService implements TimerService {
        private final long watermark;

        private TestTimerService(long watermark) {
            this.watermark = watermark;
        }

        @Override
        public long currentProcessingTime() {
            throw new UnsupportedOperationException("Not used by unit tests");
        }

        @Override
        public long currentWatermark() {
            return watermark;
        }

        @Override
        public void registerProcessingTimeTimer(long time) {
            throw new UnsupportedOperationException("Not used by unit tests");
        }

        @Override
        public void registerEventTimeTimer(long time) {
            throw new UnsupportedOperationException("Not used by unit tests");
        }

        @Override
        public void deleteProcessingTimeTimer(long time) {
            throw new UnsupportedOperationException("Not used by unit tests");
        }

        @Override
        public void deleteEventTimeTimer(long time) {
            throw new UnsupportedOperationException("Not used by unit tests");
        }
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
