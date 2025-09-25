package com.annihilator.data.playground.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.core.Configuration;
import io.dropwizard.db.DataSourceFactory;

public class DataPhantomConfig extends Configuration {

    private DataSourceFactory database;
    private DataPhantomJwtConfig jwt;
    private AWSEmrConfig awsEmr;
    private ConnectorConfig connector;

    @JsonProperty("database")
    public DataSourceFactory getDatabase() { return database; }

    @JsonProperty("database")
    public void setDatabase(DataSourceFactory database) { this.database = database; }

    @JsonProperty("jwt")
    public DataPhantomJwtConfig getJwt() { return jwt; }

    @JsonProperty("jwt")
    public void setJwt(DataPhantomJwtConfig jwt) { this.jwt = jwt; }

    @JsonProperty("aws_emr")
    public AWSEmrConfig getAwsEmr() { return awsEmr; }

    @JsonProperty("aws_emr")
    public void setAwsEmr(AWSEmrConfig awsEmr) { this.awsEmr = awsEmr; }

    @JsonProperty("connector")
    public ConnectorConfig getConnector() { return connector; }

    @JsonProperty("connector")
    public void setConnector(ConnectorConfig connector) { this.connector = connector; }
}
