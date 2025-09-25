package com.annihilator.data.playground.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;

public class DataPhantomJwtConfig {

    @NotNull
    private String secretKey;
    
    @NotNull
    private int tokenExpirationMinutes = 60;
    
    private int refreshTokenExpirationDays = 7;

    @JsonProperty("secretKey")
    public String getSecretKey() { 
        return secretKey; 
    }

    @JsonProperty("tokenExpirationMinutes")
    public int getTokenExpirationMinutes() {
        return tokenExpirationMinutes;
    }

    @JsonProperty("refreshTokenExpirationDays")
    public int getRefreshTokenExpirationDays() {
        return refreshTokenExpirationDays;
    }


    public void setSecretKey(String secretKey) {
        this.secretKey = secretKey; 
    }


    public void setTokenExpirationMinutes(int tokenExpirationMinutes) {
        this.tokenExpirationMinutes = tokenExpirationMinutes; 
    }

    public void setRefreshTokenExpirationDays(int refreshTokenExpirationDays) {
        this.refreshTokenExpirationDays = refreshTokenExpirationDays; 
    }
}



