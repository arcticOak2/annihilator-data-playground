package com.annihilator.data.playground.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.core.Configuration;
import io.dropwizard.db.DataSourceFactory;

public class DataPhantomConfig extends Configuration {

    private DataSourceFactory database;
    private DataPhantomJwtConfig jwt;
    private ConnectorConfig connector;
    private ConcurrencyConfig concurrencyConfig;
    private NotificationConfig notification;
    private ReconciliationConfig reconciliationConfig;

    @JsonProperty("meta_store")
    public DataSourceFactory getMetaStore() { return database; }

    @JsonProperty("jwt")
    public DataPhantomJwtConfig getJwt() { return jwt; }

    @JsonProperty("connector")
    public ConnectorConfig getConnector() { return connector; }

    @JsonProperty("concurrency_config")
    public ConcurrencyConfig getConcurrencyConfig() { return concurrencyConfig; }

    @JsonProperty("notification")
    public NotificationConfig getNotification() { return notification; }

    @JsonProperty("reconciliation_settings")
    public ReconciliationConfig getReconciliationConfig() { return reconciliationConfig; }

    public void setMetaStore(DataSourceFactory database) { this.database = database; }

    public void setJwt(DataPhantomJwtConfig jwt) { this.jwt = jwt; }

    public void setConnector(ConnectorConfig connector) { this.connector = connector; }

    public void setConcurrencyConfig(ConcurrencyConfig concurrencyConfig) { this.concurrencyConfig = concurrencyConfig; }

    public void setNotification(NotificationConfig notification) { this.notification = notification; }

    public void setReconciliationConfig(ReconciliationConfig reconciliationConfig) { this.reconciliationConfig = reconciliationConfig; }
}
