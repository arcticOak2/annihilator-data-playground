package com.annihilator.data.playground.utility;

import java.time.LocalTime;
import java.time.ZoneId;

public class DataPhantomUtility {

  public static int parseTimeComponent(String component) {
    if (component.equals("*")) return 0; // Default to 0 for wildcard

    if (component.startsWith("*/")) {
      try {
        return Integer.parseInt(component.substring(2));
      } catch (NumberFormatException e) {
        return 0;
      }
    }

    try {
      return Integer.parseInt(component);
    } catch (NumberFormatException e) {
      return 0;
    }
  }

  public static long getExecutionTimeInMillis(String cronExpression) {

    if (cronExpression == null || cronExpression.trim().isEmpty()) {
      return Long.MAX_VALUE; // Manual playgrounds come last
    }

    try {
      String[] parts = cronExpression.trim().split("\\s+");

      // Handle 5-part vs 6-part cron
      int offset = parts.length == 6 ? 1 : 0;

      // Parse minutes and hours
      int minutes = parseTimeComponent(parts[offset]); // minutes
      int hours = parseTimeComponent(parts[offset + 1]); // hours

      // Convert to milliseconds from midnight
      long totalMillis = (hours * 60 * 60 * 1000L) + (minutes * 60 * 1000L);

      return totalMillis;

    } catch (Exception e) {
      return Long.MAX_VALUE; // Invalid cron comes last
    }
  }

  public static long getCurrentTimeInMillis() {

    LocalTime now = LocalTime.now(ZoneId.of("UTC"));
    int hour = now.getHour();
    int minute = now.getMinute();
    int second = now.getSecond();

    long timeInMillis = (hour * 60 * 60 * 1000L) + (minute * 60 * 1000L) + (second * 1000L);

    return timeInMillis;
  }

  /**
   * Calculates the next execution time in milliseconds from midnight for interval-based cron
   * expressions. This method handles cron expressions with intervals and returns the next
   * occurrence based on the current time.
   *
   * @param cronExpression The cron expression to parse
   * @return The next execution time in milliseconds from midnight, or Long.MAX_VALUE for invalid
   *     expressions, or -1 if no execution today
   */
  public static long getNextExecutionTimeInMillis(String cronExpression) {

    if (cronExpression == null || cronExpression.trim().isEmpty()) {
      return Long.MAX_VALUE; // Manual playgrounds come last
    }

    try {
      String[] parts = cronExpression.trim().split("\\s+");

      // Handle 5-part vs 6-part cron
      int offset = parts.length == 6 ? 1 : 0;

      String minutesPart = parts[offset];
      String hoursPart = parts[offset + 1];

      long currentTime = getCurrentTimeInMillis();

      if (minutesPart.startsWith("*/") && hoursPart.startsWith("*/")) {
        int minuteInterval = parseTimeComponent(minutesPart);
        int hourInterval = parseTimeComponent(hoursPart);

        if (minuteInterval == 0 || hourInterval == 0) {
          return Long.MAX_VALUE; // Invalid interval
        }

        int totalMinuteInterval = hourInterval * 60;
        int intervalMinutes = Math.min(minuteInterval, totalMinuteInterval);
        long intervalMillis = intervalMinutes * 60 * 1000L;

        long intervalsPassed = currentTime / intervalMillis;
        long nextExecution = (intervalsPassed + 1) * intervalMillis;

        // If next execution exceeds 24 hours, return -1
        if (nextExecution >= 24 * 60 * 60 * 1000L) {
          return -1;
        }

        return nextExecution;

      } else if (hoursPart.startsWith("*/")) {
        int hourInterval = parseTimeComponent(hoursPart);

        if (hourInterval == 0) {
          return Long.MAX_VALUE; // Invalid interval
        }

        int minutes = parseTimeComponent(minutesPart);

        // Find the next execution time within the next 24 hours
        for (int hour = 0; hour < 24; hour += hourInterval) {
          long executionTime = (hour * 60 * 60 * 1000L) + (minutes * 60 * 1000L);

          if (executionTime > currentTime) {
            return executionTime;
          }
        }

        // If no execution found in current day, return -1
        return -1;

      } else if (minutesPart.startsWith("*/")) {
        int minuteInterval = parseTimeComponent(minutesPart);

        if (minuteInterval == 0) {
          return Long.MAX_VALUE; // Invalid interval
        }

        int hours = parseTimeComponent(hoursPart);

        // Find the next execution time within the next 24 hours
        for (int hour = hours; hour < 24; hour++) {
          for (int minute = 0; minute < 60; minute += minuteInterval) {
            long executionTime = (hour * 60 * 60 * 1000L) + (minute * 60 * 1000L);

            if (executionTime > currentTime) {
              return executionTime;
            }
          }
        }

        // If no execution found in current day, return -1
        return -1;

      } else {
        long result = getExecutionTimeInMillis(cronExpression);
        // If original method returns 0 (invalid), return Long.MAX_VALUE
        return result == 0 ? Long.MAX_VALUE : result;
      }

    } catch (Exception e) {
      return Long.MAX_VALUE; // Invalid cron comes last
    }
  }
}
