package com.annihilator.data.playground.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.NotNull;

public class ReconciliationConfig {

    @NotNull
    @DecimalMin(value = "0.001", message = "False positive rate must be at least 0.001 (0.1%)")
    @DecimalMax(value = "0.5", message = "False positive rate cannot exceed 0.5 (50%)")
    private double falsePositiveRate;

    @NotNull
    @Min(value = 1, message = "Estimated rows must be at least 1")
    @Max(value = 10000000, message = "Estimated rows cannot exceed 10,000,000")
    private int estimatedRows;

    @NotNull
    @Min(value = 1024, message = "Exact match threshold must be at least 1024 bytes (1KB)")
    @Max(value = 1073741824, message = "Exact match threshold cannot exceed 1073741824 bytes (1GB)")
    private long exactMatchThreshold;

    @JsonProperty("false_positive_rate")
    public double getFalsePositiveRate() {
        return falsePositiveRate;
    }

    @JsonProperty("estimated_rows")
    public int getEstimatedRows() {
        return estimatedRows;
    }

    @JsonProperty("exact_match_threshold")
    public long getExactMatchThreshold() {
        return exactMatchThreshold;
    }

    public void setFalsePositiveRate(double falsePositiveRate) {
        this.falsePositiveRate = falsePositiveRate;
    }

    public void setEstimatedRows(int estimatedRows) {
        this.estimatedRows = estimatedRows;
    }

    public void setExactMatchThreshold(long exactMatchThreshold) {
        this.exactMatchThreshold = exactMatchThreshold;
    }
}
