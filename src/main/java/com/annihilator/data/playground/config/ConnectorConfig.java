package com.annihilator.data.playground.config;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ConnectorConfig {

  private MySQLConnectorConfig mysql;

  private AWSEmrConfig awsEmrConfig;

  @JsonProperty("mysql")
  public MySQLConnectorConfig getMysql() {
    return mysql;
  }

  @JsonProperty("aws_emr")
  public AWSEmrConfig getAwsEmrConfig() {
    return awsEmrConfig;
  }

  public void setMysql(MySQLConnectorConfig mysql) {
    this.mysql = mysql;
  }

  public void setAwsEmrConfig(AWSEmrConfig awsEmrConfig) {
    this.awsEmrConfig = awsEmrConfig;
  }
}
