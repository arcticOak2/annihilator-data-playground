package com.annihilator.data.playground.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;

public class DataPhantomDBConfig {

    @NotNull
    private String url;
    private String user;
    private String password;

    @JsonProperty("url")
    public String getUrl() { return url; }

    @JsonProperty("user")
    public String getUser() { return user; }

    @JsonProperty("password")
    public String getPassword() { return password; }

    public void setUrl(String url) { this.url = url; }
    public void setUser(String user) { this.user = user; }
    public void setPassword(String password) { this.password = password; }
}
