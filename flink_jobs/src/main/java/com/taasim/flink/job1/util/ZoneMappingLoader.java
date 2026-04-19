package com.taasim.flink.job1.util;

import com.taasim.flink.job1.model.ZoneDefinition;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Loads Casablanca arrondissement zone definitions from the {@code zone_mapping.csv} classpath resource.
 *
 * <p>This project treats {@code metadata/zone_mapping.csv} as the canonical source of truth. The Maven
 * build for {@code flink_jobs} packages that file into the job JAR so Flink can load it at runtime.
 *
 * <p>Expected columns (header row):
 *
 * <pre>
 * arrondissement_id,zone_name,lon_min,lon_max,lat_min,lat_max
 * </pre>
 */
public final class ZoneMappingLoader {
    private static final String RESOURCE_NAME = "zone_mapping.csv";
    private static final String EXPECTED_HEADER =
            "arrondissement_id,zone_name,lon_min,lon_max,lat_min,lat_max";
    private static final int EXPECTED_COLUMNS = 6;

    private ZoneMappingLoader() {}

    /**
     * Loads zones from the job classpath.
     *
     * @return non-empty list of {@link ZoneDefinition}
     * @throws IllegalStateException if the resource is missing or cannot be parsed into any zones
     */
    public static List<ZoneDefinition> loadZonesFromClasspath() {
        final ClassLoader classLoader = ZoneMappingLoader.class.getClassLoader();
        final URL resourceUrl = classLoader.getResource(RESOURCE_NAME);
        if (resourceUrl == null) {
            throw new IllegalStateException(RESOURCE_NAME + " not found on classpath");
        }

        final List<ZoneDefinition> zones = new ArrayList<>();
        int dataLineCount = 0;
        int skippedMalformedLineCount = 0;
        String observedHeader = null;
        char delimiter = ',';

        try (InputStream in = resourceUrl.openStream();
                BufferedReader reader =
                        new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
            String line;
            boolean header = true;
            int lineNumber = 0;
            while ((line = reader.readLine()) != null) {
                lineNumber++;
                line = line.trim();
                if (line.isEmpty() || line.startsWith("#")) {
                    continue;
                }
                if (header) {
                    observedHeader = line;
                    delimiter = detectDelimiterFromHeader(line);
                    header = false;
                    continue;
                }

                dataLineCount++;

                // arrondissement_id,zone_name,lon_min,lon_max,lat_min,lat_max
                String[] parts = splitLine(line, delimiter);

                // Fallback when the header delimiter doesn't match (common when CSV is ';' delimited)
                if (parts.length < EXPECTED_COLUMNS && delimiter != ';') {
                    final String[] semicolonParts = splitLine(line, ';');
                    if (semicolonParts.length >= EXPECTED_COLUMNS) {
                        parts = semicolonParts;
                    }
                }

                if (parts.length < EXPECTED_COLUMNS) {
                    skippedMalformedLineCount++;
                    continue;
                }

                try {
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
                } catch (RuntimeException parseEx) {
                    throw new IllegalStateException(
                            "Invalid row in "
                                    + RESOURCE_NAME
                                    + " at line "
                                    + lineNumber
                                    + ": "
                                    + line,
                            parseEx);
                }
            }
        } catch (IOException ioEx) {
            throw new RuntimeException(
                    "Failed to load zone mapping CSV from classpath resource: " + resourceUrl, ioEx);
        }

        if (zones.isEmpty()) {
            throw new IllegalStateException(
                    RESOURCE_NAME
                            + " loaded but contained 0 zones (resource="
                            + resourceUrl
                            + ", observedHeader="
                            + (observedHeader == null ? "<missing>" : observedHeader)
                            + ", dataLines="
                            + dataLineCount
                            + ", skippedMalformedLines="
                            + skippedMalformedLineCount
                            + ", expectedHeader="
                            + EXPECTED_HEADER
                            + ")");
        }
        return zones;
    }

    private static char detectDelimiterFromHeader(String headerLine) {
        if (headerLine.indexOf(',') >= 0) {
            return ',';
        }
        if (headerLine.indexOf(';') >= 0) {
            return ';';
        }
        return ',';
    }

    private static String[] splitLine(String line, char delimiter) {
        return line.split(delimiter == ';' ? ";" : ",", -1);
    }
}
