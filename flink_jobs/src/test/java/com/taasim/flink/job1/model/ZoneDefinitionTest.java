package com.taasim.flink.job1.model;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class ZoneDefinitionTest {

    @Test
    void contains_isInclusiveOnBounds() {
        final ZoneDefinition z = new ZoneDefinition();
        z.lonMin = -1.0;
        z.lonMax = 1.0;
        z.latMin = 10.0;
        z.latMax = 20.0;

        assertTrue(z.contains(0.0, 15.0));
        assertTrue(z.contains(-1.0, 10.0));
        assertTrue(z.contains(1.0, 20.0));
    }

    @Test
    void contains_rejectsOutsideBounds() {
        final ZoneDefinition z = new ZoneDefinition();
        z.lonMin = -1.0;
        z.lonMax = 1.0;
        z.latMin = 10.0;
        z.latMax = 20.0;

        assertFalse(z.contains(-1.0001, 15.0));
        assertFalse(z.contains(1.0001, 15.0));
        assertFalse(z.contains(0.0, 9.9999));
        assertFalse(z.contains(0.0, 20.0001));
    }
}
