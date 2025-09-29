package com.annihilator.data.playground.db;

import io.dropwizard.db.DataSourceFactory;
import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetaDBConnection {

  private static final Logger logger = LoggerFactory.getLogger(MetaDBConnection.class);

  private final DataSource dataSource;

  public MetaDBConnection(
      DataSourceFactory dataSourceFactory, io.dropwizard.core.setup.Environment environment) {
    this.dataSource = dataSourceFactory.build(environment.metrics(), "database");
  }

  public Connection getConnection() throws SQLException {
    Connection connection = dataSource.getConnection();
    logger.debug("Retrieved database connection from pool: {}", connection);
    return connection;
  }
}
