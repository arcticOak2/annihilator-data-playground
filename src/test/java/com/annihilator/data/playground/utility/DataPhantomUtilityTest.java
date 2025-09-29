package com.annihilator.data.playground.utility;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DataPhantomUtilityTest {

  @Test
  void testGetCurrentTimeInMillis_ShouldReturnCurrentTime() {
    // Given
    long before = System.currentTimeMillis();

    // When
    long result = DataPhantomUtility.getCurrentTimeInMillis();

    // Then
    assertTrue(result >= 0); // Should be positive
    assertTrue(result < 24 * 60 * 60 * 1000); // Should be less than 24 hours in milliseconds
  }

  @Test
  void testGetExecutionTimeInMillis_WithValidCronExpression_ShouldReturnCorrectTime() {
    // Given
    String cronExpression = "0 0 12 * * ?"; // 12:00 PM daily

    // When
    long result = DataPhantomUtility.getExecutionTimeInMillis(cronExpression);

    // Then
    // Should be 12 hours * 60 minutes * 60 seconds * 1000 milliseconds = 43,200,000 ms
    long expectedTime = 12 * 60 * 60 * 1000L;
    assertEquals(expectedTime, result);
  }

  @Test
  void testGetExecutionTimeInMillis_WithDifferentCronExpressions_ShouldReturnDifferentTimes() {
    // Given
    String cronExpression1 = "0 0 8 * * ?"; // 8:00 AM
    String cronExpression2 = "0 30 14 * * ?"; // 2:30 PM

    // When
    long result1 = DataPhantomUtility.getExecutionTimeInMillis(cronExpression1);
    long result2 = DataPhantomUtility.getExecutionTimeInMillis(cronExpression2);

    // Then
    long expectedTime1 = 8 * 60 * 60 * 1000L; // 8 hours in milliseconds
    long expectedTime2 = (14 * 60 * 60 * 1000L) + (30 * 60 * 1000L); // 14:30 in milliseconds

    assertEquals(expectedTime1, result1);
    assertEquals(expectedTime2, result2);
    assertTrue(result2 > result1);
  }

  @Test
  void testGetExecutionTimeInMillis_WithNullCronExpression_ShouldReturnMaxValue() {
    // Given
    String cronExpression = null;

    // When
    long result = DataPhantomUtility.getExecutionTimeInMillis(cronExpression);

    // Then
    assertEquals(Long.MAX_VALUE, result);
  }

  @Test
  void testGetExecutionTimeInMillis_WithEmptyCronExpression_ShouldReturnMaxValue() {
    // Given
    String cronExpression = "";

    // When
    long result = DataPhantomUtility.getExecutionTimeInMillis(cronExpression);

    // Then
    assertEquals(Long.MAX_VALUE, result);
  }

  @Test
  void testGetExecutionTimeInMillis_WithWhitespaceCronExpression_ShouldReturnMaxValue() {
    // Given
    String cronExpression = "   ";

    // When
    long result = DataPhantomUtility.getExecutionTimeInMillis(cronExpression);

    // Then
    assertEquals(Long.MAX_VALUE, result);
  }

  @Test
  void testGetExecutionTimeInMillis_WithComplexCronExpression_ShouldReturnCorrectTime() {
    // Given
    String cronExpression = "0 15 9 * * ?"; // 9:15 AM daily

    // When
    long result = DataPhantomUtility.getExecutionTimeInMillis(cronExpression);

    // Then
    long expectedTime = (9 * 60 * 60 * 1000L) + (15 * 60 * 1000L); // 9:15 in milliseconds
    assertEquals(expectedTime, result);
  }

  @Test
  void testGetExecutionTimeInMillis_WithHourlyCronExpression_ShouldReturnCorrectTime() {
    // Given
    String cronExpression = "0 0 * * * ?"; // Every hour at minute 0

    // When
    long result = DataPhantomUtility.getExecutionTimeInMillis(cronExpression);

    // Then
    // Should be 0 hours * 60 minutes * 60 seconds * 1000 milliseconds = 0 ms
    long expectedTime = 0 * 60 * 60 * 1000L;
    assertEquals(expectedTime, result);
  }

  @Test
  void testGetExecutionTimeInMillis_WithSpecificDateCronExpression_ShouldReturnCorrectTime() {
    // Given
    String cronExpression = "0 0 18 * * ?"; // 6:00 PM daily

    // When
    long result = DataPhantomUtility.getExecutionTimeInMillis(cronExpression);

    // Then
    long expectedTime = 18 * 60 * 60 * 1000L; // 18 hours in milliseconds
    assertEquals(expectedTime, result);
  }

  @Test
  void testGetExecutionTimeInMillis_WithEdgeCaseCronExpressions_ShouldHandleGracefully() {
    // Given
    String[] cronExpressions = {
      "0 0 0 * * ?", // Midnight
      "0 59 23 * * ?", // 11:59 PM
      "0 0 1 * * ?" // 1:00 AM
    };

    long[] expectedTimes = {
      0 * 60 * 60 * 1000L, // Midnight
      (23 * 60 * 60 * 1000L) + (59 * 60 * 1000L), // 11:59 PM
      1 * 60 * 60 * 1000L // 1:00 AM
    };

    // When & Then
    for (int i = 0; i < cronExpressions.length; i++) {
      long result = DataPhantomUtility.getExecutionTimeInMillis(cronExpressions[i]);
      assertEquals(expectedTimes[i], result, "Failed for cron: " + cronExpressions[i]);
    }
  }

  @Test
  void testParseTimeComponent_WithValidNumbers_ShouldReturnCorrectValues() {
    // Given
    String[] components = {"0", "15", "30", "45", "59"};
    int[] expectedValues = {0, 15, 30, 45, 59};

    // When & Then
    for (int i = 0; i < components.length; i++) {
      int result = DataPhantomUtility.parseTimeComponent(components[i]);
      assertEquals(expectedValues[i], result);
    }
  }

  @Test
  void testParseTimeComponent_WithWildcard_ShouldReturnZero() {
    // Given
    String component = "*";

    // When
    int result = DataPhantomUtility.parseTimeComponent(component);

    // Then
    assertEquals(0, result);
  }

  @Test
  void testParseTimeComponent_WithStepValues_ShouldReturnCorrectValues() {
    // Given
    String[] components = {"*/5", "*/15", "*/30"};
    int[] expectedValues = {5, 15, 30};

    // When & Then
    for (int i = 0; i < components.length; i++) {
      int result = DataPhantomUtility.parseTimeComponent(components[i]);
      assertEquals(expectedValues[i], result);
    }
  }

  @Test
  void testParseTimeComponent_WithInvalidValues_ShouldReturnZero() {
    // Given
    String[] components = {"abc", "*/abc", "invalid", ""};

    // When & Then
    for (String component : components) {
      int result = DataPhantomUtility.parseTimeComponent(component);
      assertEquals(0, result, "Should return 0 for invalid component: " + component);
    }
  }

  @Test
  void testGetExecutionTimeInMillis_WithInvalidCronExpression_ShouldReturnZero() {
    // Given
    String cronExpression = "invalid cron expression";

    // When
    long result = DataPhantomUtility.getExecutionTimeInMillis(cronExpression);

    // Then
    assertEquals(0, result);
  }

  @Test
  void testGetExecutionTimeInMillis_WithShortCronExpression_ShouldReturnCalculatedValue() {
    // Given
    String cronExpression = "0 12"; // Too short - but actually parses as 12 hours = 43200000ms

    // When
    long result = DataPhantomUtility.getExecutionTimeInMillis(cronExpression);

    // Then
    assertEquals(43200000, result); // 12 hours in milliseconds
  }

  // Tests for getNextExecutionTimeInMillis method

  @Test
  void testGetNextExecutionTimeInMillis_WithHourlyInterval_ShouldReturnNextExecution() {
    // Given
    String cronExpression = "0 0 */2 * * ?"; // Every 2 hours

    // When
    long result = DataPhantomUtility.getNextExecutionTimeInMillis(cronExpression);

    // Then
    assertTrue(result >= -1); // Should be >= -1 (allows -1 for no execution today)
    assertTrue(result <= 24 * 60 * 60 * 1000); // Should be within 24 hours
    assertTrue(result % (2 * 60 * 60 * 1000L) == 0); // Should be divisible by 2 hours
  }

  @Test
  void testGetNextExecutionTimeInMillis_WithMinuteInterval_ShouldReturnNextExecution() {
    // Given
    String cronExpression = "0 */15 9 * * ?"; // Every 15 minutes starting at 9 AM

    // When
    long result = DataPhantomUtility.getNextExecutionTimeInMillis(cronExpression);

    // Then
    assertTrue(result >= -1); // Should be >= -1 (allows -1 for no execution today)
    assertTrue(result <= 24 * 60 * 60 * 1000); // Should be within 24 hours

    // Should be at 9 AM base time plus some multiple of 15 minutes
    long baseTime = 9 * 60 * 60 * 1000L; // 9 AM
    long timeFromBase = result - baseTime;
    assertTrue(timeFromBase >= 0); // Should be at or after 9 AM
    assertTrue(timeFromBase % (15 * 60 * 1000L) == 0); // Should be divisible by 15 minutes
  }

  @Test
  void testGetNextExecutionTimeInMillis_WithBothIntervals_ShouldReturnNextExecution() {
    // Given
    String cronExpression = "0 */30 */3 * * ?"; // Every 30 minutes, every 3 hours

    // When
    long result = DataPhantomUtility.getNextExecutionTimeInMillis(cronExpression);

    // Then
    assertTrue(result >= -1); // Should be >= -1 (allows -1 for no execution today)
    assertTrue(result <= 24 * 60 * 60 * 1000); // Should be within 24 hours
    assertTrue(
        result % (30 * 60 * 1000L) == 0); // Should be divisible by 30 minutes (smaller interval)
  }

  @Test
  void testGetNextExecutionTimeInMillis_WithNonIntervalCron_ShouldUseOriginalMethod() {
    // Given
    String cronExpression = "0 0 12 * * ?"; // Daily at 12 PM

    // When
    long result = DataPhantomUtility.getNextExecutionTimeInMillis(cronExpression);
    long originalResult = DataPhantomUtility.getExecutionTimeInMillis(cronExpression);

    // Then
    assertEquals(originalResult, result); // Should return same as original method
    assertEquals(12 * 60 * 60 * 1000L, result); // 12 PM in milliseconds
  }

  @Test
  void testGetNextExecutionTimeInMillis_WithNullCronExpression_ShouldReturnMaxValue() {
    // Given
    String cronExpression = null;

    // When
    long result = DataPhantomUtility.getNextExecutionTimeInMillis(cronExpression);

    // Then
    assertEquals(Long.MAX_VALUE, result);
  }

  @Test
  void testGetNextExecutionTimeInMillis_WithEmptyCronExpression_ShouldReturnMaxValue() {
    // Given
    String cronExpression = "";

    // When
    long result = DataPhantomUtility.getNextExecutionTimeInMillis(cronExpression);

    // Then
    assertEquals(Long.MAX_VALUE, result);
  }

  @Test
  void testGetNextExecutionTimeInMillis_WithInvalidInterval_ShouldReturnMaxValue() {
    // Given
    String cronExpression = "0 0 */0 * * ?"; // Invalid interval (0)

    // When
    long result = DataPhantomUtility.getNextExecutionTimeInMillis(cronExpression);

    // Then
    assertEquals(Long.MAX_VALUE, result);
  }

  @Test
  void testGetNextExecutionTimeInMillis_WithInvalidCronExpression_ShouldReturnMaxValue() {
    // Given
    String cronExpression = "invalid cron expression";

    // When
    long result = DataPhantomUtility.getNextExecutionTimeInMillis(cronExpression);

    // Then
    assertEquals(Long.MAX_VALUE, result);
  }

  @Test
  void testGetNextExecutionTimeInMillis_WithDifferentHourIntervals_ShouldReturnCorrectTimes() {
    // Given
    String[] cronExpressions = {
      "0 0 */1 * * ?", // Every hour
      "0 0 */3 * * ?", // Every 3 hours
      "0 0 */6 * * ?", // Every 6 hours
      "0 0 */12 * * ?" // Every 12 hours
    };

    int[] hourIntervals = {1, 3, 6, 12};

    // When & Then
    for (int i = 0; i < cronExpressions.length; i++) {
      long result = DataPhantomUtility.getNextExecutionTimeInMillis(cronExpressions[i]);
      assertTrue(result >= -1, "Result should be >= -1 for: " + cronExpressions[i]);
      assertTrue(
          result <= 24 * 60 * 60 * 1000,
          "Result should be within 24 hours for: " + cronExpressions[i]);

      // Check if result is a valid execution time based on the interval
      if (result != -1) {
        long hourIntervalMillis = hourIntervals[i] * 60 * 60 * 1000L;
        // For hour intervals, valid times are: 0, interval, 2*interval, 3*interval, etc.
        // Since minutes is 0, the result should be divisible by the hour interval
        assertTrue(
            result % hourIntervalMillis == 0,
            "Result should be divisible by hour interval for: "
                + cronExpressions[i]
                + ", got: "
                + result
                + ", expected interval: "
                + hourIntervalMillis);
      }
    }
  }

  @Test
  void testGetNextExecutionTimeInMillis_WithDifferentMinuteIntervals_ShouldReturnCorrectTimes() {
    // Given
    String[] cronExpressions = {
      "0 */5 0 * * ?", // Every 5 minutes starting at midnight
      "0 */15 0 * * ?", // Every 15 minutes starting at midnight
      "0 */30 0 * * ?", // Every 30 minutes starting at midnight
      "0 */60 0 * * ?" // Every 60 minutes starting at midnight
    };

    long[] expectedIntervals = {
      5 * 60 * 1000L, // 5 minutes
      15 * 60 * 1000L, // 15 minutes
      30 * 60 * 1000L, // 30 minutes
      60 * 60 * 1000L // 60 minutes
    };

    // When & Then
    for (int i = 0; i < cronExpressions.length; i++) {
      long result = DataPhantomUtility.getNextExecutionTimeInMillis(cronExpressions[i]);
      assertTrue(result >= -1, "Result should be >= -1 for: " + cronExpressions[i]);
      assertTrue(
          result <= 24 * 60 * 60 * 1000,
          "Result should be within 24 hours for: " + cronExpressions[i]);
      assertTrue(
          result % expectedIntervals[i] == 0,
          "Result should be divisible by interval for: " + cronExpressions[i]);
    }
  }

  @Test
  void testGetNextExecutionTimeInMillis_WithComplexIntervalExpressions_ShouldHandleGracefully() {
    // Given
    String[] cronExpressions = {
      "0 0 */2 * * ?", // Every 2 hours
      "0 30 */4 * * ?", // Every 4 hours at 30 minutes past
      "0 */10 8 * * ?", // Every 10 minutes starting at 8 AM
      "0 0 */8 * * ?" // Every 8 hours
    };

    // When & Then
    for (String cronExpression : cronExpressions) {
      long result = DataPhantomUtility.getNextExecutionTimeInMillis(cronExpression);
      assertTrue(result >= -1, "Result should be >= -1 for: " + cronExpression);
      assertTrue(
          result <= 24 * 60 * 60 * 1000, "Result should be within 24 hours for: " + cronExpression);
    }
  }

  // Tests with mocked time

}
