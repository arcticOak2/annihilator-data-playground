package com.annihilator.data.playground.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class StatusTest {

    @Test
    void testStatusValues_ShouldContainAllExpectedStatuses() {
        // Given
        Status[] values = Status.values();
        
        // Then
        assertEquals(10, values.length);
        assertTrue(java.util.Arrays.asList(values).contains(Status.PENDING));
        assertTrue(java.util.Arrays.asList(values).contains(Status.RUNNING));
        assertTrue(java.util.Arrays.asList(values).contains(Status.SUCCESS));
        assertTrue(java.util.Arrays.asList(values).contains(Status.FAILED));
        assertTrue(java.util.Arrays.asList(values).contains(Status.CANCELLED));
        assertTrue(java.util.Arrays.asList(values).contains(Status.IDLE));
        assertTrue(java.util.Arrays.asList(values).contains(Status.PARTIAL_SUCCESS));
        assertTrue(java.util.Arrays.asList(values).contains(Status.UNKNOWN));
        assertTrue(java.util.Arrays.asList(values).contains(Status.SKIPPED));
        assertTrue(java.util.Arrays.asList(values).contains(Status.UPSTREAM_FAILED));
    }

    @Test
    void testStatusValues_ShouldBeImmutable() {
        // Given
        Status[] values = Status.values();
        int originalLength = values.length;
        
        // When
        try {
            // Try to modify the array (this should not affect the enum)
            values[0] = null;
        } catch (Exception e) {
            // Expected - arrays returned by values() are typically read-only
        }
        
        // Then
        assertEquals(originalLength, Status.values().length);
        assertNotNull(Status.values()[0]);
    }

    @Test
    void testStatusEnum_ShouldHaveCorrectEnumConstants() {
        // Given
        Status[] values = Status.values();
        
        // Then
        assertEquals(Status.PENDING, values[0]);
        assertEquals(Status.RUNNING, values[1]);
        assertEquals(Status.SUCCESS, values[2]);
        assertEquals(Status.FAILED, values[3]);
        assertEquals(Status.CANCELLED, values[4]);
        assertEquals(Status.IDLE, values[5]);
        assertEquals(Status.PARTIAL_SUCCESS, values[6]);
        assertEquals(Status.UNKNOWN, values[7]);
        assertEquals(Status.SKIPPED, values[8]);
        assertEquals(Status.UPSTREAM_FAILED, values[9]);
    }

    @Test
    void testStatusValueOf_ShouldReturnCorrectStatus() {
        // Given
        String[] statusNames = {
            "PENDING", "RUNNING", "SUCCESS", "FAILED", "CANCELLED",
            "IDLE", "PARTIAL_SUCCESS", "UNKNOWN", "SKIPPED", "UPSTREAM_FAILED"
        };
        
        Status[] expectedStatuses = {
            Status.PENDING, Status.RUNNING, Status.SUCCESS, Status.FAILED, Status.CANCELLED,
            Status.IDLE, Status.PARTIAL_SUCCESS, Status.UNKNOWN, Status.SKIPPED, Status.UPSTREAM_FAILED
        };
        
        // When & Then
        for (int i = 0; i < statusNames.length; i++) {
            Status result = Status.valueOf(statusNames[i]);
            assertEquals(expectedStatuses[i], result);
        }
    }

    @Test
    void testStatusValueOf_WithInvalidName_ShouldThrowException() {
        // Given
        String invalidName = "INVALID_STATUS";
        
        // When & Then
        assertThrows(IllegalArgumentException.class, () -> Status.valueOf(invalidName));
    }

    @Test
    void testStatusValueOf_WithNullName_ShouldThrowException() {
        // Given
        String nullName = null;
        
        // When & Then
        assertThrows(NullPointerException.class, () -> Status.valueOf(nullName));
    }

    @Test
    void testStatusOrdinal_ShouldHaveCorrectOrder() {
        // Then
        assertEquals(0, Status.PENDING.ordinal());
        assertEquals(1, Status.RUNNING.ordinal());
        assertEquals(2, Status.SUCCESS.ordinal());
        assertEquals(3, Status.FAILED.ordinal());
        assertEquals(4, Status.CANCELLED.ordinal());
        assertEquals(5, Status.IDLE.ordinal());
        assertEquals(6, Status.PARTIAL_SUCCESS.ordinal());
        assertEquals(7, Status.UNKNOWN.ordinal());
        assertEquals(8, Status.SKIPPED.ordinal());
        assertEquals(9, Status.UPSTREAM_FAILED.ordinal());
    }

    @Test
    void testStatusName_ShouldReturnCorrectName() {
        // Then
        assertEquals("PENDING", Status.PENDING.name());
        assertEquals("RUNNING", Status.RUNNING.name());
        assertEquals("SUCCESS", Status.SUCCESS.name());
        assertEquals("FAILED", Status.FAILED.name());
        assertEquals("CANCELLED", Status.CANCELLED.name());
        assertEquals("IDLE", Status.IDLE.name());
        assertEquals("PARTIAL_SUCCESS", Status.PARTIAL_SUCCESS.name());
        assertEquals("UNKNOWN", Status.UNKNOWN.name());
        assertEquals("SKIPPED", Status.SKIPPED.name());
        assertEquals("UPSTREAM_FAILED", Status.UPSTREAM_FAILED.name());
    }

    @Test
    void testStatusToString_ShouldReturnName() {
        // Then
        assertEquals("PENDING", Status.PENDING.toString());
        assertEquals("RUNNING", Status.RUNNING.toString());
        assertEquals("SUCCESS", Status.SUCCESS.toString());
        assertEquals("FAILED", Status.FAILED.toString());
        assertEquals("CANCELLED", Status.CANCELLED.toString());
        assertEquals("IDLE", Status.IDLE.toString());
        assertEquals("PARTIAL_SUCCESS", Status.PARTIAL_SUCCESS.toString());
        assertEquals("UNKNOWN", Status.UNKNOWN.toString());
        assertEquals("SKIPPED", Status.SKIPPED.toString());
        assertEquals("UPSTREAM_FAILED", Status.UPSTREAM_FAILED.toString());
    }
}