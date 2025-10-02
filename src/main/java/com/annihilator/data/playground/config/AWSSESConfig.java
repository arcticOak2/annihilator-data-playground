package com.annihilator.data.playground.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotNull;

public class AWSSESConfig {

    @NotNull(message = "AWS SES access key cannot be null")
    private String accessKey;

    @NotNull(message = "AWS SES secret key cannot be null")
    private String secretKey;

    @NotNull(message = "From email address cannot be null")
    @Email(message = "From email address must be a valid email format")
    private String from;

    @NotNull(message = "To email address cannot be null")
    @Email(message = "To email address must be a valid email format")
    private String to;

    @JsonProperty("access_key")
    public String getAccessKey() {
        return accessKey;
    }

    @JsonProperty("secret_key")
    public String getSecretKey() {
        return secretKey;
    }

    @JsonProperty("from")
    public String getFrom() {
        return from;
    }

    @JsonProperty("to")
    public String getTo() {
        return to;
    }

    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    public void setSecretKey(String secretKey) {
        this.secretKey = secretKey;
    }

    public void setFrom(String from) {
        this.from = from;
    }

    public void setTo(String to) {
        this.to = to;
    }
}

