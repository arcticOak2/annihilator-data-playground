package com.annihilator.data.playground.db;

import com.annihilator.data.playground.model.CSVComparisonResult;
import com.annihilator.data.playground.model.ReconciliationResultResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class ReconciliationResultsDAOImpl implements ReconciliationResultsDAO {

    private static final Logger logger = LoggerFactory.getLogger(ReconciliationResultsDAOImpl.class);
    
    private final MetaDBConnection metaDBConnection;

    public ReconciliationResultsDAOImpl(MetaDBConnection metaDBConnection) {
        this.metaDBConnection = metaDBConnection;
    }

    @Override
    public void upsertReconciliationResult(String reconciliationId, CSVComparisonResult result, String status, String matchType) throws SQLException {
        if (reconciliationId == null || reconciliationId.trim().isEmpty()) {
            throw new IllegalArgumentException("Reconciliation ID cannot be null or empty");
        }
        if (result == null) {
            throw new IllegalArgumentException("CSVComparisonResult cannot be null");
        }
        if (status == null || status.trim().isEmpty()) {
            throw new IllegalArgumentException("Status cannot be null or empty");
        }

        try (Connection conn = metaDBConnection.getConnection()) {
            // First, delete if exists
            String deleteSql = "DELETE FROM reconciliation_results WHERE reconciliation_id = ?";
            try (PreparedStatement deletePs = conn.prepareStatement(deleteSql)) {
                deletePs.setString(1, reconciliationId);
                int deletedRows = deletePs.executeUpdate();
                if (deletedRows > 0) {
                    logger.info("Deleted existing reconciliation result for ID: {}", reconciliationId);
                }
            }

            // Then insert new record
            String insertSql = "INSERT INTO reconciliation_results (" +
                    "reconciliation_id, status, left_file_row_count, right_file_row_count, " +
                    "common_row_count, left_file_exclusive_row_count, right_file_exclusive_row_count, " +
                    "sample_common_rows_s3_path, sample_exclusive_left_rows_s3_path, " +
                    "sample_exclusive_right_rows_s3_path, reconciliation_method" +
                    ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

            try (PreparedStatement insertPs = conn.prepareStatement(insertSql)) {
                insertPs.setString(1, reconciliationId);
                insertPs.setString(2, status); // Use provided status
                insertPs.setInt(3, result.getLeftFileRowCount());
                insertPs.setInt(4, result.getRightFileRowCount());
                insertPs.setInt(5, result.getCommonRowCount());
                insertPs.setInt(6, result.getLeftFileExclusiveRowCount());
                insertPs.setInt(7, result.getRightFileExclusiveRowCount());
                insertPs.setString(8, result.getSampleCommonRowsS3Path());
                insertPs.setString(9, result.getSampleExclusiveLeftRowsS3Path());
                insertPs.setString(10, result.getSampleExclusiveRightRowsS3Path());
                insertPs.setString(11, matchType);

                insertPs.executeUpdate();
                logger.info("Inserted reconciliation result for reconciliation ID: {} with status: {}", reconciliationId, status);
            }
            
        } catch (SQLException e) {
            logger.error("Failed to upsert reconciliation result for ID: {}", reconciliationId, e);
            throw e;
        }
    }

    @Override
    public ReconciliationResultResponse getReconciliationResult(String reconciliationId) throws SQLException {
        if (reconciliationId == null || reconciliationId.trim().isEmpty()) {
            throw new IllegalArgumentException("Reconciliation ID cannot be null or empty");
        }

        String sql = "SELECT reconciliation_id, status, left_file_row_count, right_file_row_count, " +
                "common_row_count, left_file_exclusive_row_count, right_file_exclusive_row_count, " +
                "sample_common_rows_s3_path, sample_exclusive_left_rows_s3_path, " +
                "sample_exclusive_right_rows_s3_path, reconciliation_method, execution_timestamp " +
                "FROM reconciliation_results WHERE reconciliation_id = ?";

        try (Connection conn = metaDBConnection.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            
            ps.setString(1, reconciliationId);

            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    ReconciliationResultResponse response = new ReconciliationResultResponse();
                    response.setReconciliationId(rs.getString("reconciliation_id"));
                    response.setStatus(rs.getString("status"));
                    response.setExecutionTimestamp(rs.getTimestamp("execution_timestamp"));
                    response.setReconciliationMethod(rs.getString("reconciliation_method"));
                    response.setLeftFileRowCount(rs.getInt("left_file_row_count"));
                    response.setRightFileRowCount(rs.getInt("right_file_row_count"));
                    response.setCommonRowCount(rs.getInt("common_row_count"));
                    response.setLeftFileExclusiveRowCount(rs.getInt("left_file_exclusive_row_count"));
                    response.setRightFileExclusiveRowCount(rs.getInt("right_file_exclusive_row_count"));
                    response.setSampleCommonRowsS3Path(rs.getString("sample_common_rows_s3_path"));
                    response.setSampleExclusiveLeftRowsS3Path(rs.getString("sample_exclusive_left_rows_s3_path"));
                    response.setSampleExclusiveRightRowsS3Path(rs.getString("sample_exclusive_right_rows_s3_path"));

                    logger.info("Retrieved reconciliation result for ID: {} with status: {}", 
                            reconciliationId, rs.getString("status"));
                    return response;
                }
            }
            
            logger.info("No reconciliation result found for ID: {}", reconciliationId);
            
        } catch (SQLException e) {
            logger.error("Failed to retrieve reconciliation result for ID: {}", reconciliationId, e);
            throw e;
        }

        return null;
    }
}
