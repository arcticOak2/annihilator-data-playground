package com.annihilator.data.playground.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Max;

public class AWSEmrConfig {

    @NotNull
    private String region;
    @NotNull
    private String stackName;
    @NotNull
    private String accessKey;
    @NotNull
    private String secretKey;
    @NotNull
    private String s3Bucket;
    
    private String s3PathPrefix = "data-phantom";

    @NotNull
    private String clusterLogicalId;

    @NotNull
    @Min(value = 1, message = "Max step retries must be at least 1")
    @Max(value = 10, message = "Max step retries cannot exceed 10")
    private int maxStepRetries;

    @NotNull
    @Min(value = 1, message = "S3 max keys per request must be at least 1")
    @Max(value = 1000, message = "S3 max keys per request cannot exceed 1000")
    private int s3MaxKeysPerRequest;

    @NotNull
    @Min(value = 1, message = "Stack update check max attempt must be at least 1")
    @Max(value = 300, message = "Stack update check max attempt cannot exceed 300")
    private int stackUpdateCheckMaxAttempt;

    @NotNull
    @Min(value = 1, message = "S3 output preview line count must be at least 1")
    @Max(value = 10000, message = "S3 output preview line count cannot exceed 10000")
    private int s3OutputPreviewLineCount;

    @NotNull
    @Min(value = 1000, message = "Stack update polling interval must be at least 1000ms (1 second)")
    @Max(value = 300000, message = "Stack update polling interval cannot exceed 300000ms (5 minutes)")
    private long stackUpdatePollingInterval;

    @NotNull
    @Min(value = 1000, message = "Step polling interval must be at least 1000ms (1 second)")
    @Max(value = 300000, message = "Step polling interval cannot exceed 300000ms (5 minutes)")
    private long stepPollingInterval;

    @JsonProperty("region")
    public String getRegion() {
        return region;
    }

    @JsonProperty("stack_name")
    public String getStackName() {
        return stackName;
    }

    @JsonProperty("access_key")
    public String getAccessKey() {
        return accessKey;
    }

    @JsonProperty("secret_key")
    public String getSecretKey() {
        return secretKey;
    }

    @JsonProperty("s3_bucket")
    public String getS3Bucket() {
        return s3Bucket;
    }
    
    @JsonProperty("s3_path_prefix")
    public String getS3PathPrefix() {
        return s3PathPrefix;
    }

    @JsonProperty("max_step_retries")
    public int getMaxStepRetries() {
        return maxStepRetries;
    }

    @JsonProperty("cluster_logical_id")
    public String getClusterLogicalId() {
        return clusterLogicalId;
    }

    @JsonProperty("s3_max_keys_per_request")
    public int getS3MaxKeysPerRequest() {
        return s3MaxKeysPerRequest;
    }

    @JsonProperty("stack_update_check_max_attempt")
    public int getStackUpdateCheckMaxAttempt() {
        return stackUpdateCheckMaxAttempt;
    }

    @JsonProperty("s3_output_preview_line_count")
    public int getS3OutputPreviewLineCount() {
        return s3OutputPreviewLineCount;
    }

    @JsonProperty("stack_update_polling_interval")
    public long getStackUpdatePollingInterval() {
        return stackUpdatePollingInterval;
    }

    @JsonProperty("step_polling_interval")
    public long getStepPollingInterval() {
        return stepPollingInterval;
    }

    public void setS3MaxKeysPerRequest(int s3MaxKeysPerRequest) {
        this.s3MaxKeysPerRequest = s3MaxKeysPerRequest;
    }

    public void setStackUpdateCheckMaxAttempt(int stackUpdateCheckMaxAttempt) {
        this.stackUpdateCheckMaxAttempt = stackUpdateCheckMaxAttempt;
    }

    public void setS3OutputPreviewLineCount(int s3OutputPreviewLineCount) {
        this.s3OutputPreviewLineCount = s3OutputPreviewLineCount;
    }

    public void setStepPollingInterval(long stepPollingInterval) {
        this.stepPollingInterval = stepPollingInterval;
    }

    public void setStackUpdatePollingInterval(long stackUpdatePollingInterval) {
        this.stackUpdatePollingInterval = stackUpdatePollingInterval;
    }

    public void setClusterLogicalId(String clusterLogicalId) {
        this.clusterLogicalId = clusterLogicalId;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public void setStackName(String stackName) {
        this.stackName = stackName;
    }

    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    public void setSecretKey(String secretKey) {
        this.secretKey = secretKey;
    }

    public void setS3Bucket(String s3Bucket) {
        this.s3Bucket = s3Bucket;
    }
    
    public void setS3PathPrefix(String s3PathPrefix) {
        this.s3PathPrefix = s3PathPrefix;
    }

    public void setMaxStepRetries(int maxStepRetries) {
        this.maxStepRetries = maxStepRetries;
    }
}
