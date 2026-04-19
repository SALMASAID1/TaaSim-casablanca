package com.taasim.flink.job1.util;

import com.taasim.flink.job1.model.ZoneDefinition;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public final class ZoneMappingLoader {
    private ZoneMappingLoader() {}

    public static List<ZoneDefinition> loadZonesFromClasspath() {
        final InputStream in =
                ZoneMappingLoader.class.getClassLoader().getResourceAsStream("zone_mapping.csv");
        if (in == null) {
            throw new IllegalStateException("zone_mapping.csv not found on classpath");
        }

        final List<ZoneDefinition> zones = new ArrayList<>();

        try (BufferedReader reader =
                new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
            String line;
            boolean header = true;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) {
                    continue;
                }
                if (header) {
                    header = false;
                    continue;
                }

                // arrondissement_id,zone_name,lon_min,lon_max,lat_min,lat_max
                final String[] parts = line.split(",", -1);
                if (parts.length < 6) {
                    continue;
                }

                final ZoneDefinition z = new ZoneDefinition();
                z.arrondissementId = Integer.parseInt(parts[0].trim());
                z.zoneName = parts[1].trim();
                z.lonMin = Double.parseDouble(parts[2].trim());
                z.lonMax = Double.parseDouble(parts[3].trim());
                z.latMin = Double.parseDouble(parts[4].trim());
                z.latMax = Double.parseDouble(parts[5].trim());
                z.centroidLon = (z.lonMin + z.lonMax) / 2.0;
                z.centroidLat = (z.latMin + z.latMax) / 2.0;
                zones.add(z);
            }
        } catch (Exception ex) {
            throw new RuntimeException("Failed to load zone mapping CSV", ex);
        }

        if (zones.isEmpty()) {
            throw new IllegalStateException("zone_mapping.csv loaded but contained 0 zones");
        }
        return zones;
    }
}
