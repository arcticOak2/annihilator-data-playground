package com.annihilator.data.playground.config;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ReconciliationConfig {

    private double falsePositiveRate;

    private int estimatedRows;

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
