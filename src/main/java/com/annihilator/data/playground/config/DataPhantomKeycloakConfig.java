package com.annihilator.data.playground.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;

public class DataPhantomKeycloakConfig {

    @NotNull
    private String url;
    @NotNull
    private String realm;
    @NotNull
    private String clientId;
    @NotNull
    private String clientSecret;
    private String sslRequired;
    private boolean bearerOnly;
    private boolean userResourceRoleMapping;

    @JsonProperty("url")
    public String getUrl() { return url; }

    @JsonProperty("realm")
    public String getRealm() { return realm; }

    @JsonProperty("clientId")
    public String getClientId() { return clientId; }

    @JsonProperty("clientSecret")
    public String getClientSecret() { return clientSecret; }

    @JsonProperty("sslRequired")
    public String getSslRequired() { return sslRequired; }

    @JsonProperty("bearerOnly")
    public boolean isBearerOnly() { return bearerOnly; }

    @JsonProperty("userResourceRoleMapping")
    public boolean isUserResourceRoleMapping() { return userResourceRoleMapping; }

    public void setUrl(String url) { this.url = url; }
    public void setRealm(String realm) { this.realm = realm; }
    public void setClientId(String clientId) { this.clientId = clientId; }
    public void setClientSecret(String clientSecret) { this.clientSecret = clientSecret; }
    public void setSslRequired(String sslRequired) { this.sslRequired = sslRequired; }
    public void setBearerOnly(boolean bearerOnly) { this.bearerOnly = bearerOnly; }
    public void setUserResourceRoleMapping(boolean userResourceRoleMapping) { this.userResourceRoleMapping = userResourceRoleMapping; }
}
