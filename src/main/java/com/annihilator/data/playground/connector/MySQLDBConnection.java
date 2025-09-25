package com.annihilator.data.playground.connector;

import com.annihilator.data.playground.config.MySQLConnectorConfig;
import io.dropwizard.db.DataSourceFactory;
import io.dropwizard.core.setup.Environment;
import io.dropwizard.util.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

public class MySQLDBConnection {
    
    private static final Logger logger = LoggerFactory.getLogger(MySQLDBConnection.class);
    
    private final DataSource dataSource;
    
    public MySQLDBConnection(MySQLConnectorConfig config, Environment environment) {
        DataSourceFactory dataSourceFactory = new DataSourceFactory();
        dataSourceFactory.setDriverClass(config.getDriverClass());
        dataSourceFactory.setUrl(config.getUrl());
        dataSourceFactory.setUser(config.getUser());
        dataSourceFactory.setPassword(config.getPassword());
        
        dataSourceFactory.setMaxSize(config.getMaxSize());
        dataSourceFactory.setMinSize(config.getMinSize());
        dataSourceFactory.setMaxWaitForConnection(Duration.parse(config.getMaxWaitForConnection()));
        dataSourceFactory.setMaxConnectionAge(Duration.parse(config.getMaxConnectionAge()));
        dataSourceFactory.setMinIdleTime(Duration.parse(config.getMinIdleTime()));
        dataSourceFactory.setValidationQuery(config.getValidationQuery());
        dataSourceFactory.setValidationQueryTimeout(Duration.parse(config.getValidationQueryTimeout()));
        dataSourceFactory.setCheckConnectionOnBorrow(config.isCheckConnectionOnBorrow());
        dataSourceFactory.setCheckConnectionOnReturn(config.isCheckConnectionOnReturn());
        
        this.dataSource = dataSourceFactory.build(environment.metrics(), "mysql-connector");
        logger.info("MySQL connection pool initialized with maxSize: {}, minSize: {}", 
                   config.getMaxSize(), config.getMinSize());
    }
    
    public Connection getConnection() throws SQLException {
        Connection connection = dataSource.getConnection();
        logger.debug("Retrieved MySQL connection from pool: {}", connection);
        return connection;
    }
}
