package com.taasim.flink.job1.util;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.taasim.flink.job1.model.ZoneDefinition;
import java.util.List;
import org.junit.jupiter.api.Test;

class ZoneMappingLoaderTest {

    @Test
    void loadZonesFromClasspath_returnsNonEmptyZonesWithValidCentroids() {
        final List<ZoneDefinition> zones = ZoneMappingLoader.loadZonesFromClasspath();
        assertNotNull(zones);
        assertFalse(zones.isEmpty());

        for (ZoneDefinition z : zones) {
            assertTrue(z.arrondissementId > 0);
            assertNotNull(z.zoneName);
            assertFalse(z.zoneName.isBlank());
            assertTrue(z.lonMin <= z.lonMax);
            assertTrue(z.latMin <= z.latMax);
            assertTrue(z.contains(z.centroidLon, z.centroidLat));
        }
    }
}
