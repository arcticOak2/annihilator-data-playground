package com.annihilator.data.playground.config;

/**
 * Configuration class for EMR task retry behavior
 */
public class RetryConfig {
    
    private int maxRetries = 3;
    private long baseDelayMs = 5000; // 5 seconds
    private double backoffMultiplier = 2.0;
    private long maxDelayMs = 60000; // 1 minute
    private boolean enableJitter = true;
    private double jitterFactor = 0.1; // 10% jitter
    
    public RetryConfig() {
        // Default constructor with default values
    }
    
    public RetryConfig(int maxRetries, long baseDelayMs, double backoffMultiplier, 
                      long maxDelayMs, boolean enableJitter, double jitterFactor) {
        this.maxRetries = maxRetries;
        this.baseDelayMs = baseDelayMs;
        this.backoffMultiplier = backoffMultiplier;
        this.maxDelayMs = maxDelayMs;
        this.enableJitter = enableJitter;
        this.jitterFactor = jitterFactor;
    }
    
    /**
     * Calculate the delay for a given attempt number
     * Uses exponential backoff with optional jitter
     */
    public long getRetryDelay(int attempt) {
        // Calculate exponential backoff delay
        long delay = (long) (baseDelayMs * Math.pow(backoffMultiplier, attempt));
        
        // Cap at maximum delay
        delay = Math.min(delay, maxDelayMs);
        
        // Add jitter if enabled (random variation to prevent thundering herd)
        if (enableJitter) {
            double jitter = (Math.random() - 0.5) * 2 * jitterFactor; // -jitterFactor to +jitterFactor
            delay = (long) (delay * (1 + jitter));
            // Re-cap after jitter to ensure we don't exceed maxDelayMs
            delay = Math.min(delay, maxDelayMs);
        }
        
        return Math.max(delay, 100); // Minimum 100ms delay
    }
    
    /**
     * Check if a failure should be retried based on the error message
     */
    public boolean shouldRetry(String errorMessage) {
        if (errorMessage == null) {
            return true; // Retry unknown errors
        }
        
        String lowerError = errorMessage.toLowerCase();
        
        // Don't retry these types of errors (permanent failures)
        if (lowerError.contains("syntax error") ||
            lowerError.contains("invalid query") ||
            lowerError.contains("table not found") ||
            lowerError.contains("column not found") ||
            lowerError.contains("permission denied") ||
            lowerError.contains("authentication failed") ||
            lowerError.contains("access denied")) {
            return false;
        }
        
        // Retry these types of errors (transient failures)
        if (lowerError.contains("timeout") ||
            lowerError.contains("connection") ||
            lowerError.contains("network") ||
            lowerError.contains("temporary") ||
            lowerError.contains("resource") ||
            lowerError.contains("throttle") ||
            lowerError.contains("rate limit") ||
            lowerError.contains("service unavailable")) {
            return true;
        }
        
        // Default to retry for unknown errors
        return true;
    }
    
    // Getters and setters
    public int getMaxRetries() {
        return maxRetries;
    }
    
    public void setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
    }
    
    public long getBaseDelayMs() {
        return baseDelayMs;
    }
    
    public void setBaseDelayMs(long baseDelayMs) {
        this.baseDelayMs = baseDelayMs;
    }
    
    public double getBackoffMultiplier() {
        return backoffMultiplier;
    }
    
    public void setBackoffMultiplier(double backoffMultiplier) {
        this.backoffMultiplier = backoffMultiplier;
    }
    
    public long getMaxDelayMs() {
        return maxDelayMs;
    }
    
    public void setMaxDelayMs(long maxDelayMs) {
        this.maxDelayMs = maxDelayMs;
    }
    
    public boolean isEnableJitter() {
        return enableJitter;
    }
    
    public void setEnableJitter(boolean enableJitter) {
        this.enableJitter = enableJitter;
    }
    
    public double getJitterFactor() {
        return jitterFactor;
    }
    
    public void setJitterFactor(double jitterFactor) {
        this.jitterFactor = jitterFactor;
    }
    
    @Override
    public String toString() {
        return "RetryConfig{" +
                "maxRetries=" + maxRetries +
                ", baseDelayMs=" + baseDelayMs +
                ", backoffMultiplier=" + backoffMultiplier +
                ", maxDelayMs=" + maxDelayMs +
                ", enableJitter=" + enableJitter +
                ", jitterFactor=" + jitterFactor +
                '}';
    }
}
