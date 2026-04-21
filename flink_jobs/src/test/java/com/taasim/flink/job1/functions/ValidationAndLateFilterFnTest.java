package com.taasim.flink.job1.functions;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class ValidationAndLateFilterFnTest {

    @Test
    void isInCasablancaBbox_matchesContract() {
        assertTrue(ValidationAndLateFilterFn.isInCasablancaBbox(-7.6, 33.6));
        assertTrue(ValidationAndLateFilterFn.isInCasablancaBbox(-7.8, 33.4));
        assertTrue(ValidationAndLateFilterFn.isInCasablancaBbox(-7.4, 33.7));

        assertFalse(ValidationAndLateFilterFn.isInCasablancaBbox(-7.81, 33.6));
        assertFalse(ValidationAndLateFilterFn.isInCasablancaBbox(-7.39, 33.6));
        assertFalse(ValidationAndLateFilterFn.isInCasablancaBbox(-7.6, 33.39));
        assertFalse(ValidationAndLateFilterFn.isInCasablancaBbox(-7.6, 33.71));
    }

    @Test
    void isSpeedValid_dropsAbove150() {
        assertTrue(ValidationAndLateFilterFn.isSpeedValid(0f));
        assertTrue(ValidationAndLateFilterFn.isSpeedValid(150f));
        assertFalse(ValidationAndLateFilterFn.isSpeedValid(150.0001f));
    }

    @Test
    void isLate_matchesContract() {
        assertFalse(ValidationAndLateFilterFn.isLate(1000L, Long.MIN_VALUE));
        assertFalse(ValidationAndLateFilterFn.isLate(1000L, 1000L));
        assertTrue(ValidationAndLateFilterFn.isLate(999L, 1000L));
    }
}
