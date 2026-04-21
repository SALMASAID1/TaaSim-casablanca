package com.taasim.flink.job1.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.taasim.flink.job1.model.ZoneDefinition;
import java.util.Comparator;
import java.util.List;
import org.junit.jupiter.api.Test;

class ZoneMappingLoaderTest {

    @Test
    void loadZonesFromClasspath_loadsAllZonesAndComputesCentroids() {
        final List<ZoneDefinition> zones = ZoneMappingLoader.loadZonesFromClasspath();

        assertEquals(16, zones.size(), "Expected 16 Casablanca zones");

        final ZoneDefinition z1 =
                zones.stream()
                        .min(Comparator.comparingInt(z -> z.arrondissementId))
                        .orElseThrow();

        assertNotNull(z1.zoneName);
        assertTrue(z1.arrondissementId >= 1 && z1.arrondissementId <= 16);

        assertEquals((z1.lonMin + z1.lonMax) / 2.0, z1.centroidLon, 1e-9);
        assertEquals((z1.latMin + z1.latMax) / 2.0, z1.centroidLat, 1e-9);
        assertTrue(z1.contains(z1.centroidLon, z1.centroidLat), "Centroid must be inside bbox");
    }
}
