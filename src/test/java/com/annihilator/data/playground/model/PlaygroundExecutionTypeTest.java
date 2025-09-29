package com.annihilator.data.playground.model;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class PlaygroundExecutionTypeTest {

  @Test
  void testExecutionTypeValues_ShouldContainAllExpectedTypes() {
    // Given
    PlaygroundExecutionType[] values = PlaygroundExecutionType.values();

    // Then
    assertEquals(3, values.length);
    assertTrue(java.util.Arrays.asList(values).contains(PlaygroundExecutionType.SCHEDULED));
    assertTrue(java.util.Arrays.asList(values).contains(PlaygroundExecutionType.AD_HOC));
    assertTrue(java.util.Arrays.asList(values).contains(PlaygroundExecutionType.RECOVERY));
  }

  @Test
  void testExecutionTypeValues_ShouldBeImmutable() {
    // Given
    PlaygroundExecutionType[] values = PlaygroundExecutionType.values();
    int originalLength = values.length;

    // When
    try {
      // Try to modify the array (this should not affect the enum)
      values[0] = null;
    } catch (Exception e) {
      // Expected - arrays returned by values() are typically read-only
    }

    // Then
    assertEquals(originalLength, PlaygroundExecutionType.values().length);
    assertNotNull(PlaygroundExecutionType.values()[0]);
  }

  @Test
  void testExecutionTypeEnum_ShouldHaveCorrectEnumConstants() {
    // Given
    PlaygroundExecutionType[] values = PlaygroundExecutionType.values();

    // Then
    assertEquals(PlaygroundExecutionType.SCHEDULED, values[0]);
    assertEquals(PlaygroundExecutionType.AD_HOC, values[1]);
    assertEquals(PlaygroundExecutionType.RECOVERY, values[2]);
  }

  @Test
  void testExecutionTypeValueOf_ShouldReturnCorrectType() {
    // Given
    String[] typeNames = {"SCHEDULED", "AD_HOC", "RECOVERY"};

    PlaygroundExecutionType[] expectedTypes = {
      PlaygroundExecutionType.SCHEDULED,
      PlaygroundExecutionType.AD_HOC,
      PlaygroundExecutionType.RECOVERY
    };

    // When & Then
    for (int i = 0; i < typeNames.length; i++) {
      PlaygroundExecutionType result = PlaygroundExecutionType.valueOf(typeNames[i]);
      assertEquals(expectedTypes[i], result);
    }
  }

  @Test
  void testExecutionTypeValueOf_WithInvalidName_ShouldThrowException() {
    // Given
    String invalidName = "INVALID_TYPE";

    // When & Then
    assertThrows(
        IllegalArgumentException.class, () -> PlaygroundExecutionType.valueOf(invalidName));
  }

  @Test
  void testExecutionTypeValueOf_WithNullName_ShouldThrowException() {
    // Given
    String nullName = null;

    // When & Then
    assertThrows(NullPointerException.class, () -> PlaygroundExecutionType.valueOf(nullName));
  }

  @Test
  void testExecutionTypeOrdinal_ShouldHaveCorrectOrder() {
    // Then
    assertEquals(0, PlaygroundExecutionType.SCHEDULED.ordinal());
    assertEquals(1, PlaygroundExecutionType.AD_HOC.ordinal());
    assertEquals(2, PlaygroundExecutionType.RECOVERY.ordinal());
  }

  @Test
  void testExecutionTypeName_ShouldReturnCorrectName() {
    // Then
    assertEquals("SCHEDULED", PlaygroundExecutionType.SCHEDULED.name());
    assertEquals("AD_HOC", PlaygroundExecutionType.AD_HOC.name());
    assertEquals("RECOVERY", PlaygroundExecutionType.RECOVERY.name());
  }

  @Test
  void testExecutionTypeToString_ShouldReturnName() {
    // Then
    assertEquals("SCHEDULED", PlaygroundExecutionType.SCHEDULED.toString());
    assertEquals("AD_HOC", PlaygroundExecutionType.AD_HOC.toString());
    assertEquals("RECOVERY", PlaygroundExecutionType.RECOVERY.toString());
  }
}
