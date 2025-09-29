package com.annihilator.data.playground.config;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class RetryConfig {

  private int maxRetries = 3;
  private long baseDelayMs = 5000; // 5 seconds
  private double backoffMultiplier = 2.0;
  private long maxDelayMs = 60000; // 1 minute
  private boolean enableJitter = true;
  private double jitterFactor = 0.1; // 10% jitter

  public long getRetryDelay(int attempt) {

    long delay = (long) (baseDelayMs * Math.pow(backoffMultiplier, attempt));

    delay = Math.min(delay, maxDelayMs);

    if (enableJitter) {
      double jitter = (Math.random() - 0.5) * 2 * jitterFactor; // -jitterFactor to +jitterFactor
      delay = (long) (delay * (1 + jitter));
      delay = Math.min(delay, maxDelayMs);
    }

    return Math.max(delay, 100); // Minimum 100ms delay
  }

  public boolean shouldRetry(String errorMessage) {
    if (errorMessage == null) {
      return true;
    }

    String lowerError = errorMessage.toLowerCase();

    if (lowerError.contains("syntax error")
        || lowerError.contains("invalid query")
        || lowerError.contains("table not found")
        || lowerError.contains("column not found")
        || lowerError.contains("permission denied")
        || lowerError.contains("authentication failed")
        || lowerError.contains("access denied")) {

      return false;
    }

    if (lowerError.contains("timeout")
        || lowerError.contains("connection")
        || lowerError.contains("network")
        || lowerError.contains("temporary")
        || lowerError.contains("resource")
        || lowerError.contains("throttle")
        || lowerError.contains("rate limit")
        || lowerError.contains("service unavailable")) {

      return true;
    }

    return true;
  }

  @Override
  public String toString() {
    return "RetryConfig{"
        + "maxRetries="
        + maxRetries
        + ", baseDelayMs="
        + baseDelayMs
        + ", backoffMultiplier="
        + backoffMultiplier
        + ", maxDelayMs="
        + maxDelayMs
        + ", enableJitter="
        + enableJitter
        + ", jitterFactor="
        + jitterFactor
        + '}';
  }
}
