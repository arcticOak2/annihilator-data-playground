package com.annihilator.data.playground.config;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class for RetryConfig
 */
public class RetryConfigTest {

    @Test
    public void testDefaultConfiguration() {
        RetryConfig config = new RetryConfig();
        
        assertEquals(3, config.getMaxRetries());
        assertEquals(5000, config.getBaseDelayMs());
        assertEquals(2.0, config.getBackoffMultiplier());
        assertEquals(60000, config.getMaxDelayMs());
        assertTrue(config.isEnableJitter());
        assertEquals(0.1, config.getJitterFactor());
    }

    @Test
    public void testRetryDelayCalculation() {
        RetryConfig config = new RetryConfig();
        
        // Test exponential backoff
        long delay1 = config.getRetryDelay(0); // First retry
        long delay2 = config.getRetryDelay(1); // Second retry
        long delay3 = config.getRetryDelay(2); // Third retry
        
        // Should be exponential: 5s, 10s, 20s (with jitter)
        assertTrue(delay1 >= 4500 && delay1 <= 5500); // 5s ± 10% jitter
        assertTrue(delay2 >= 9000 && delay2 <= 11000); // 10s ± 10% jitter
        assertTrue(delay3 >= 18000 && delay3 <= 22000); // 20s ± 10% jitter
    }

    @Test
    public void testRetryDelayMaxCap() {
        RetryConfig config = new RetryConfig();
        
        // Test that delay is capped at maxDelayMs
        long delay = config.getRetryDelay(10); // Very high attempt number
        assertTrue(delay <= config.getMaxDelayMs());
    }

    @Test
    public void testShouldRetryTransientFailures() {
        RetryConfig config = new RetryConfig();
        
        // Should retry transient failures
        assertTrue(config.shouldRetry("Connection timeout"));
        assertTrue(config.shouldRetry("Network error"));
        assertTrue(config.shouldRetry("Service temporarily unavailable"));
        assertTrue(config.shouldRetry("Rate limit exceeded"));
        assertTrue(config.shouldRetry("Resource not available"));
    }

    @Test
    public void testShouldNotRetryPermanentFailures() {
        RetryConfig config = new RetryConfig();
        
        // Should not retry permanent failures
        assertFalse(config.shouldRetry("Syntax error in query"));
        assertFalse(config.shouldRetry("Table not found"));
        assertFalse(config.shouldRetry("Column not found"));
        assertFalse(config.shouldRetry("Permission denied"));
        assertFalse(config.shouldRetry("Authentication failed"));
        assertFalse(config.shouldRetry("Access denied"));
    }

    @Test
    public void testShouldRetryUnknownErrors() {
        RetryConfig config = new RetryConfig();
        
        // Should retry unknown errors (default behavior)
        assertTrue(config.shouldRetry("Unknown error"));
        assertTrue(config.shouldRetry(null));
        assertTrue(config.shouldRetry(""));
    }

    @Test
    public void testCustomConfiguration() {
        RetryConfig config = new RetryConfig(5, 10000, 1.5, 30000, false, 0.0);
        
        assertEquals(5, config.getMaxRetries());
        assertEquals(10000, config.getBaseDelayMs());
        assertEquals(1.5, config.getBackoffMultiplier());
        assertEquals(30000, config.getMaxDelayMs());
        assertFalse(config.isEnableJitter());
        assertEquals(0.0, config.getJitterFactor());
        
        // Test delay calculation without jitter
        long delay1 = config.getRetryDelay(0);
        long delay2 = config.getRetryDelay(1);
        
        assertEquals(10000, delay1); // No jitter
        assertEquals(15000, delay2); // 10s * 1.5
    }
}
