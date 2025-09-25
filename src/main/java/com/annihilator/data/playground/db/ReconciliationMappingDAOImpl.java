package com.annihilator.data.playground.db;

import com.annihilator.data.playground.model.Reconciliation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class ReconciliationMappingDAOImpl implements ReconciliationMappingDAO {

    private static final Logger logger = LoggerFactory.getLogger(ReconciliationMappingDAOImpl.class);
    
    private final MetaDBConnection metaDBConnection;

    public ReconciliationMappingDAOImpl(MetaDBConnection metaDBConnection) {
        this.metaDBConnection = metaDBConnection;
    }

    @Override
    public void createReconciliationMapping(String playgroundId, String leftTableId, String rightTableId, String map) throws SQLException {
        // Input validation
        if (playgroundId == null || playgroundId.trim().isEmpty()) {
            throw new IllegalArgumentException("Playground ID cannot be null or empty");
        }
        if (leftTableId == null || leftTableId.trim().isEmpty()) {
            throw new IllegalArgumentException("Left table ID cannot be null or empty");
        }
        if (rightTableId == null || rightTableId.trim().isEmpty()) {
            throw new IllegalArgumentException("Right table ID cannot be null or empty");
        }
        if (map == null || map.trim().isEmpty()) {
            throw new IllegalArgumentException("Map cannot be null or empty");
        }

        String sql = "INSERT INTO reconciliation_mappings (reconciliation_id, playground_id, left_table_id, right_table_id, map, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)";

        try (Connection conn = metaDBConnection.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            
            UUID uuid = UUID.randomUUID();
            ps.setString(1, uuid.toString());
            ps.setString(2, playgroundId);
            ps.setString(3, leftTableId);
            ps.setString(4, rightTableId);
            ps.setString(5, map);
            long currentTime = System.currentTimeMillis();
            ps.setLong(6, currentTime);
            ps.setLong(7, currentTime);

            ps.executeUpdate();
            logger.info("Created reconciliation mapping: {} for playground: {}", uuid, playgroundId);
            
        } catch (SQLException e) {
            logger.error("Failed to create reconciliation mapping for playground: {}", playgroundId, e);
            throw e;
        }
    }

    @Override
    public List<Reconciliation> findReconciliationMappingByPlaygroundId(String playgroundId) throws SQLException {
        // Input validation
        if (playgroundId == null || playgroundId.trim().isEmpty()) {
            throw new IllegalArgumentException("Playground ID cannot be null or empty");
        }

        String sql = "SELECT reconciliation_id, playground_id, left_table_id, right_table_id, map, created_at, updated_at FROM reconciliation_mappings WHERE playground_id = ?";

        List<Reconciliation> allReconciliation = new ArrayList<>();

        try (Connection conn = metaDBConnection.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setString(1, playgroundId);

            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    Reconciliation reconciliation = new Reconciliation();
                    reconciliation.setReconciliationId(UUID.fromString(rs.getString("reconciliation_id")));
                    reconciliation.setPlaygroundId(rs.getString("playground_id"));
                    reconciliation.setLeftTableId(rs.getString("left_table_id"));
                    reconciliation.setRightTableId(rs.getString("right_table_id"));
                    reconciliation.setMapping(rs.getString("map"));
                    reconciliation.setCreatedAt(rs.getLong("created_at"));
                    reconciliation.setUpdatedAt(rs.getLong("updated_at"));

                    allReconciliation.add(reconciliation);
                }
            }
            
            logger.info("Found {} reconciliation mappings for playground: {}", allReconciliation.size(), playgroundId);
            
        } catch (SQLException e) {
            logger.error("Failed to find reconciliation mappings for playground: {}", playgroundId, e);
            throw e;
        }

        return allReconciliation;
    }

    @Override
    public Reconciliation findReconciliationMappingById(String reconciliationId) throws SQLException {
        // Input validation
        if (reconciliationId == null || reconciliationId.trim().isEmpty()) {
            throw new IllegalArgumentException("Reconciliation ID cannot be null or empty");
        }

        String sql = "SELECT reconciliation_id, playground_id, left_table_id, right_table_id, map, created_at, updated_at FROM reconciliation_mappings WHERE reconciliation_id = ?";

        try (Connection conn = metaDBConnection.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setString(1, reconciliationId);

            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    Reconciliation reconciliation = new Reconciliation();
                    reconciliation.setReconciliationId(UUID.fromString(rs.getString("reconciliation_id")));
                    reconciliation.setPlaygroundId(rs.getString("playground_id"));
                    reconciliation.setLeftTableId(rs.getString("left_table_id"));
                    reconciliation.setRightTableId(rs.getString("right_table_id"));
                    reconciliation.setMapping(rs.getString("map"));
                    reconciliation.setCreatedAt(rs.getLong("created_at"));
                    reconciliation.setUpdatedAt(rs.getLong("updated_at"));

                    logger.info("Found reconciliation mapping: {}", reconciliationId);
                    return reconciliation;
                }
            }
            
            logger.info("Reconciliation mapping not found: {}", reconciliationId);
            
        } catch (SQLException e) {
            logger.error("Failed to find reconciliation mapping by ID: {}", reconciliationId, e);
            throw e;
        }

        return null;
    }

    @Override
    public void deleteReconciliationMappingById(String reconciliationId) throws SQLException {
        // Input validation
        if (reconciliationId == null || reconciliationId.trim().isEmpty()) {
            throw new IllegalArgumentException("Reconciliation ID cannot be null or empty");
        }

        String sql = "DELETE FROM reconciliation_mappings WHERE reconciliation_id = ?";

        try (Connection conn = metaDBConnection.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            
            ps.setString(1, reconciliationId);

            int rowsDeleted = ps.executeUpdate();
            if (rowsDeleted > 0) {
                logger.info("Deleted reconciliation mapping: {}", reconciliationId);
            } else {
                logger.warn("Reconciliation mapping not found for deletion: {}", reconciliationId);
            }
            
        } catch (SQLException e) {
            logger.error("Failed to delete reconciliation mapping: {}", reconciliationId, e);
            throw e;
        }
    }

    @Override
    public void updateReconciliationMapping(String reconciliationId, String map) throws SQLException {
        // Input validation
        if (reconciliationId == null || reconciliationId.trim().isEmpty()) {
            throw new IllegalArgumentException("Reconciliation ID cannot be null or empty");
        }
        if (map == null || map.trim().isEmpty()) {
            throw new IllegalArgumentException("Map cannot be null or empty");
        }

        String sql = "UPDATE reconciliation_mappings SET map = ?, updated_at = ? WHERE reconciliation_id = ?";

        try (Connection conn = metaDBConnection.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            
            ps.setString(1, map);
            ps.setLong(2, System.currentTimeMillis());
            ps.setString(3, reconciliationId);

            int rowsUpdated = ps.executeUpdate();
            if (rowsUpdated > 0) {
                logger.info("Updated reconciliation mapping: {}", reconciliationId);
            } else {
                logger.warn("Reconciliation mapping not found for update: {}", reconciliationId);
            }
            
        } catch (SQLException e) {
            logger.error("Failed to update reconciliation mapping: {}", reconciliationId, e);
            throw e;
        }
    }
}
