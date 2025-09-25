package com.annihilator.data.playground.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;

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

    @JsonProperty("cluster_logical_id")
    public String getClusterLogicalId() {
        return clusterLogicalId;
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
}
