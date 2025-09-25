package com.annihilator.data.playground.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;

public class DataPhantomJwtConfig {

    @NotNull
    private String secretKey;
    
    @NotNull
    private int tokenExpirationMinutes = 60; // Default 1 hour
    
    private int refreshTokenExpirationDays = 7; // Default 7 days

    @JsonProperty("secretKey")
    public String getSecretKey() { 
        return secretKey; 
    }

    @JsonProperty("secretKey")
    public void setSecretKey(String secretKey) { 
        this.secretKey = secretKey; 
    }

    @JsonProperty("tokenExpirationMinutes")
    public int getTokenExpirationMinutes() { 
        return tokenExpirationMinutes; 
    }

    @JsonProperty("tokenExpirationMinutes")
    public void setTokenExpirationMinutes(int tokenExpirationMinutes) { 
        this.tokenExpirationMinutes = tokenExpirationMinutes; 
    }

    @JsonProperty("refreshTokenExpirationDays")
    public int getRefreshTokenExpirationDays() { 
        return refreshTokenExpirationDays; 
    }

    @JsonProperty("refreshTokenExpirationDays")
    public void setRefreshTokenExpirationDays(int refreshTokenExpirationDays) { 
        this.refreshTokenExpirationDays = refreshTokenExpirationDays; 
    }
}



