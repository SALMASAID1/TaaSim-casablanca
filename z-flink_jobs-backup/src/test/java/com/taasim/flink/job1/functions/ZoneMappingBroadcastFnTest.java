package com.taasim.flink.job1.functions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.taasim.flink.job1.model.GpsNormalizedEvent;
import com.taasim.flink.job1.model.GpsRawEvent;
import com.taasim.flink.job1.model.ZoneDefinition;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.junit.jupiter.api.Test;

class ZoneMappingBroadcastFnTest {

    @Test
    void processElement_whenZoneMatches_emitsNormalizedWithCentroid() throws Exception {
        final MapStateDescriptor<Integer, ZoneDefinition> desc =
                new MapStateDescriptor<>(
                        "zones", Types.INT, TypeInformation.of(new TypeHint<ZoneDefinition>() {}));
        final ZoneMappingBroadcastFn fn = new ZoneMappingBroadcastFn(desc, "casablanca");
        final TestCounter unmapped = new TestCounter();
        setCounter(fn, "unmappedZone", unmapped);

        final ZoneDefinition zone = new ZoneDefinition();
        zone.arrondissementId = 1;
        zone.zoneName = "Z1";
        zone.lonMin = -8.0;
        zone.lonMax = -7.0;
        zone.latMin = 33.0;
        zone.latMax = 34.0;
        zone.centroidLon = -7.5;
        zone.centroidLat = 33.5;

        final InMemoryReadOnlyBroadcastState<Integer, ZoneDefinition> zones =
                new InMemoryReadOnlyBroadcastState<>(Collections.singletonMap(1, zone));

        final BroadcastProcessFunction<GpsRawEvent, ZoneDefinition, GpsNormalizedEvent>.ReadOnlyContext ctx =
                readOnlyContext(fn, desc, zones);

        final GpsRawEvent raw = new GpsRawEvent();
        raw.taxiId = "taxi-1";
        raw.tripId = "trip-9";
        raw.status = "FREE";
        raw.timestamp = "2026-04-19T20:00:00Z";
        raw.eventTimeMillis = 1234L;
        raw.lat = 33.6;
        raw.lon = -7.6;
        raw.speedKmh = 12.0f;

        final ListCollector<GpsNormalizedEvent> out = new ListCollector<>();

        fn.processElement(raw, ctx, out);

        assertEquals(0L, unmapped.getCount());
        assertEquals(1, out.values().size());

        final GpsNormalizedEvent e = out.values().get(0);
        assertEquals("casablanca", e.city);
        assertEquals(1, e.zoneId);
        assertEquals("taxi-1", e.taxiId);
        assertEquals("trip-9", e.tripId);
        assertEquals("FREE", e.status);
        assertEquals(1234L, e.eventTimeMillis);
        assertEquals(zone.centroidLat, e.lat, 1e-9);
        assertEquals(zone.centroidLon, e.lon, 1e-9);
    }

    @Test
    void processElement_whenNoZoneMatches_incrementsCounterAndEmitsNothing() throws Exception {
        final MapStateDescriptor<Integer, ZoneDefinition> desc =
                new MapStateDescriptor<>(
                        "zones", Types.INT, TypeInformation.of(new TypeHint<ZoneDefinition>() {}));
        final ZoneMappingBroadcastFn fn = new ZoneMappingBroadcastFn(desc, "casablanca");
        final TestCounter unmapped = new TestCounter();
        setCounter(fn, "unmappedZone", unmapped);

        final InMemoryReadOnlyBroadcastState<Integer, ZoneDefinition> zones =
                new InMemoryReadOnlyBroadcastState<>(Collections.emptyMap());

        final BroadcastProcessFunction<GpsRawEvent, ZoneDefinition, GpsNormalizedEvent>.ReadOnlyContext ctx =
                readOnlyContext(fn, desc, zones);

        final GpsRawEvent raw = new GpsRawEvent();
        raw.taxiId = "taxi-1";
        raw.timestamp = "2026-04-19T20:00:00Z";
        raw.eventTimeMillis = 1234L;
        raw.lat = 33.6;
        raw.lon = -7.6;

        final ListCollector<GpsNormalizedEvent> out = new ListCollector<>();

        fn.processElement(raw, ctx, out);

        assertEquals(1L, unmapped.getCount());
        assertTrue(out.values().isEmpty());
    }

    private static BroadcastProcessFunction<GpsRawEvent, ZoneDefinition, GpsNormalizedEvent>.ReadOnlyContext
            readOnlyContext(
                    ZoneMappingBroadcastFn fn,
                    MapStateDescriptor<Integer, ZoneDefinition> desc,
                    ReadOnlyBroadcastState<Integer, ZoneDefinition> zones) {
        return fn.new ReadOnlyContext() {
            @Override
            public <K, V> ReadOnlyBroadcastState<K, V> getBroadcastState(MapStateDescriptor<K, V> stateDescriptor) {
                if (stateDescriptor == desc) {
                    @SuppressWarnings("unchecked")
                    final ReadOnlyBroadcastState<K, V> cast = (ReadOnlyBroadcastState<K, V>) zones;
                    return cast;
                }
                throw new IllegalArgumentException("Unexpected state descriptor: " + stateDescriptor.getName());
            }

            @Override
            public Long timestamp() {
                return null;
            }

            @Override
            public <X> void output(OutputTag<X> outputTag, X value) {
                // no-op
            }

            @Override
            public long currentProcessingTime() {
                return 0L;
            }

            @Override
            public long currentWatermark() {
                return Long.MIN_VALUE;
            }
        };
    }

    private static void setCounter(ZoneMappingBroadcastFn fn, String fieldName, Counter counter)
            throws Exception {
        final Field field = ZoneMappingBroadcastFn.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(fn, counter);
    }

    private static final class InMemoryReadOnlyBroadcastState<K, V> implements ReadOnlyBroadcastState<K, V> {
        private final Map<K, V> data;

        private InMemoryReadOnlyBroadcastState(Map<K, V> data) {
            this.data = new LinkedHashMap<>(data);
        }

        @Override
        public V get(K key) {
            return data.get(key);
        }

        @Override
        public boolean contains(K key) {
            return data.containsKey(key);
        }

        @Override
        public Iterable<Map.Entry<K, V>> immutableEntries() {
            return Collections.unmodifiableSet(data.entrySet());
        }

        @Override
        public void clear() {
            data.clear();
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
