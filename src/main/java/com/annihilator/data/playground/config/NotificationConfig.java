package com.annihilator.data.playground.config;

import com.fasterxml.jackson.annotation.JsonProperty;

public class NotificationConfig {

    private AWSSESConfig awsSes;

    @JsonProperty("aws_ses")
    public AWSSESConfig getAwsSes() {
        return awsSes;
    }

    public void setAwsSes(AWSSESConfig awsSes) {
        this.awsSes = awsSes;
    }
}

