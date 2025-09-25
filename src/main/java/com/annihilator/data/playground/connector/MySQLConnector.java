package com.annihilator.data.playground.connector;

import com.annihilator.data.playground.config.MySQLConnectorConfig;
import com.annihilator.data.playground.cloud.aws.S3Service;
import com.annihilator.data.playground.model.StepResult;
import com.annihilator.data.playground.model.Task;
import software.amazon.awssdk.services.emr.model.StepState;
import io.dropwizard.core.setup.Environment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class MySQLConnector {

    private static final Logger logger = LoggerFactory.getLogger(MySQLConnector.class);

    private final MySQLDBConnection mysqlDBConnection;
    private final MySQLConnectorConfig config;
    private final S3Service s3Service;
    private final String pathPrefix;

    public MySQLConnector(MySQLConnectorConfig config, Environment environment, S3Service s3Service) {
        this(config, environment, s3Service, "data-phantom");
    }
    
    public MySQLConnector(MySQLConnectorConfig config, Environment environment, S3Service s3Service, String pathPrefix) {
        this.mysqlDBConnection = new MySQLDBConnection(config, environment);
        this.config = config;
        this.s3Service = s3Service;
        this.pathPrefix = pathPrefix != null ? pathPrefix : "data-phantom";
        logger.info("MySQL connector initialized with connection pool and S3 service");
    }

    public CompletableFuture<StepResult> executeSQLTask(Task task) {

        String playgroundId = task.getPlaygroundId().toString();
        String taskId = task.getId().toString();
        String query = task.getQuery();

        return CompletableFuture.supplyAsync(() -> {
            String stepId = UUID.randomUUID().toString();
            String currentDate = LocalDate.now().toString();
            
            try {
                java.io.File outputDir = new java.io.File(config.getOutputDirectory());
                if (!outputDir.exists()) {
                    outputDir.mkdirs();
                }
                
                String fileName = String.format("sql-output-%s-%s-%s.csv", playgroundId, taskId, stepId);
                String localFilePath = new java.io.File(outputDir, fileName).getAbsolutePath();
                
                logger.info("Executing SQL task {} for playground {} with step ID {}", taskId, playgroundId, stepId);
                
                // Execute query and write to CSV
                try (Connection conn = mysqlDBConnection.getConnection()) {
                    try (Statement stmt = conn.createStatement(
                            ResultSet.TYPE_FORWARD_ONLY,
                            ResultSet.CONCUR_READ_ONLY)) {
                        
                        stmt.setFetchSize(1000);

                        try (ResultSet rs = stmt.executeQuery(query);
                             PrintWriter writer = new PrintWriter(new FileWriter(localFilePath))) {

                            ResultSetMetaData meta = rs.getMetaData();
                            int columnCount = meta.getColumnCount();

                            // Write headers
                            for (int i = 1; i <= columnCount; i++) {
                                writer.print(meta.getColumnName(i));
                                if (i < columnCount) writer.print(",");
                            }
                            writer.println();

                            // Write data rows
                            while (rs.next()) {
                                for (int i = 1; i <= columnCount; i++) {
                                    String val = formatValue(rs, i);
                                    if (val != null) {
                                        val = val.replace("\"", "\"\"");
                                        if (val.contains(",") || val.contains("\"") || val.contains("\n")) {
                                            val = "\"" + val + "\"";
                                        }
                                    }
                                    writer.print(val == null ? "" : val);
                                    if (i < columnCount) writer.print(",");
                                }
                                writer.println();
                            }

                            logger.info("SQL query executed successfully, results written to {}", localFilePath);
                        }
                    }
                }
                
                String s3ObjectKey = String.format("%s/%s/%s/%s.csv", 
                                                  pathPrefix, currentDate, playgroundId, taskId);
                String uploadedS3Key = s3Service.uploadLocalFile(localFilePath, s3ObjectKey);
                String s3OutputPath = String.format("s3://%s/%s", s3Service.getBucketName(), uploadedS3Key);
                
                // Clean up local file
                java.io.File localFile = new java.io.File(localFilePath);
                if (localFile.exists()) {
                    localFile.delete();
                    logger.debug("Cleaned up local file: {}", localFilePath);
                }
                
                logger.info("SQL task {} completed successfully, output uploaded to S3: {}", taskId, s3OutputPath);
                
                return new StepResult(stepId, StepState.COMPLETED, 
                                    "SQL task executed successfully", s3OutputPath, null, taskId);
                
            } catch (Exception e) {
                logger.error("SQL task {} failed: {}", taskId, e.getMessage(), e);
                return new StepResult(stepId, StepState.FAILED, 
                                    "SQL task failed: " + e.getMessage(), null, null, taskId);
            }
        });
    }

    public String formatValue(ResultSet rs, int columnIndex) throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();
        int columnType = meta.getColumnType(columnIndex);
        
        switch (columnType) {
            case java.sql.Types.DECIMAL:
            case java.sql.Types.NUMERIC:
            case java.sql.Types.FLOAT:
            case java.sql.Types.DOUBLE:
            case java.sql.Types.REAL:
                try {
                    BigDecimal decimal = rs.getBigDecimal(columnIndex);
                    if (decimal != null) {
                        return decimal.stripTrailingZeros().toPlainString();
                    }
                } catch (SQLException e) {
                }
                break;
        }
        
        return rs.getString(columnIndex);
    }
}