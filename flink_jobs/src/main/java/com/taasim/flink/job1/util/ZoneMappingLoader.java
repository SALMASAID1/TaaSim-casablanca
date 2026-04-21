package com.taasim.flink.job1.util;

import com.taasim.flink.job1.model.ZoneDefinition;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public final class ZoneMappingLoader {
    private ZoneMappingLoader() {}

    public static List<ZoneDefinition> loadZonesFromClasspath() {
        final InputStream stream = ZoneMappingLoader.class.getClassLoader().getResourceAsStream("zone_mapping.csv");
        if (stream == null) {
            throw new IllegalStateException(
                    "Could not find zone_mapping.csv on the classpath. Expected metadata/zone_mapping.csv to be added as a Maven resource.");
        }

        final List<ZoneDefinition> zones = new ArrayList<>();
        try (BufferedReader reader =
                new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
            String line;
            boolean headerSkipped = false;
            while ((line = reader.readLine()) != null) {
                final String trimmed = line.trim();
                if (trimmed.isEmpty()) {
                    continue;
                }
                if (!headerSkipped) {
                    headerSkipped = true;
                    continue;
                }

                final String[] parts = trimmed.split(",", -1);
                if (parts.length < 6) {
                    continue;
                }

                final Integer arrondissementId = parseIntOrNull(parts[0]);
                final String zoneName = safeString(parts[1]);
                final Double lonMin = parseDoubleOrNull(parts[2]);
                final Double lonMax = parseDoubleOrNull(parts[3]);
                final Double latMin = parseDoubleOrNull(parts[4]);
                final Double latMax = parseDoubleOrNull(parts[5]);

                if (arrondissementId == null || lonMin == null || lonMax == null || latMin == null || latMax == null) {
                    continue;
                }

                final ZoneDefinition zone = new ZoneDefinition();
                zone.arrondissementId = arrondissementId;
                zone.zoneName = zoneName;
                zone.lonMin = Math.min(lonMin, lonMax);
                zone.lonMax = Math.max(lonMin, lonMax);
                zone.latMin = Math.min(latMin, latMax);
                zone.latMax = Math.max(latMin, latMax);
                zone.centroidLon = (zone.lonMin + zone.lonMax) / 2.0;
                zone.centroidLat = (zone.latMin + zone.latMax) / 2.0;
                zones.add(zone);
            }
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read zone_mapping.csv", e);
        }

        return List.copyOf(zones);
    }

    private static Integer parseIntOrNull(String raw) {
        final String value = safeString(raw);
        if (value == null) {
            return null;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static Double parseDoubleOrNull(String raw) {
        final String value = safeString(raw);
        if (value == null) {
            return null;
        }
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static String safeString(String raw) {
        if (raw == null) {
            return null;
        }
        final String trimmed = raw.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }
}
