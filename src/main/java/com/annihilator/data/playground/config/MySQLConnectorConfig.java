package com.annihilator.data.playground.config;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class MySQLConnectorConfig {

    private String driverClass;

    private String user;

    private String password;

    private String url;
    
    private int maxSize = 20;
    private int minSize = 5;
    private String maxWaitForConnection = "30s";
    private String maxConnectionAge = "30m";
    private String minIdleTime = "10m";
    
    private String validationQuery = "SELECT 1";
    private String validationQueryTimeout = "3s";
    private boolean checkConnectionOnBorrow = true;
    private boolean checkConnectionOnReturn = true;
    
    private String outputDirectory = "/tmp/sql-output";
}
