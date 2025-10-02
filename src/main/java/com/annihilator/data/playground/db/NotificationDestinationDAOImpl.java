package com.annihilator.data.playground.db;

import com.annihilator.data.playground.model.NotificationDestination;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class NotificationDestinationDAOImpl implements NotificationDestinationDAO {

    private static final Logger logger = LoggerFactory.getLogger(NotificationDestinationDAOImpl.class);
    
    private final MetaDBConnection metaDBConnection;

    public NotificationDestinationDAOImpl(MetaDBConnection metaDBConnection) {
        this.metaDBConnection = metaDBConnection;
    }

    @Override
    public void createNotificationDestination(NotificationDestination destination) throws SQLException {
        if (destination == null) {
            throw new IllegalArgumentException("Notification destination cannot be null");
        }
        if (destination.getId() == null) {
            throw new IllegalArgumentException("Notification destination ID cannot be null");
        }
        if (destination.getPlaygroundId() == null) {
            throw new IllegalArgumentException("Playground ID cannot be null");
        }
        if (destination.getDestinationType() == null || destination.getDestinationType().trim().isEmpty()) {
            throw new IllegalArgumentException("Destination type cannot be null or empty");
        }
        if (destination.getDestination() == null || destination.getDestination().trim().isEmpty()) {
            throw new IllegalArgumentException("Destination cannot be null or empty");
        }

        String sql = "INSERT INTO notification_destinations (id, playground_id, destination_type, destination, created_at) VALUES (?, ?, ?, ?, ?)";

        try (Connection conn = metaDBConnection.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            
            ps.setString(1, destination.getId().toString());
            ps.setString(2, destination.getPlaygroundId().toString());
            ps.setString(3, destination.getDestinationType());
            ps.setString(4, destination.getDestination());
            ps.setLong(5, destination.getCreatedAt());

            ps.executeUpdate();
            logger.info("Created notification destination: {} for playground: {}", destination.getId(), destination.getPlaygroundId());
            
        } catch (SQLException e) {
            logger.error("Failed to create notification destination for playground: {}", destination.getPlaygroundId(), e);
            throw e;
        }
    }

    @Override
    public List<NotificationDestination> getNotificationDestinationsByPlaygroundId(UUID playgroundId) throws SQLException {
        if (playgroundId == null) {
            throw new IllegalArgumentException("Playground ID cannot be null");
        }

        String sql = "SELECT id, playground_id, destination_type, destination, created_at FROM notification_destinations WHERE playground_id = ? ORDER BY created_at ASC";

        List<NotificationDestination> destinations = new ArrayList<>();

        try (Connection conn = metaDBConnection.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setString(1, playgroundId.toString());

            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    NotificationDestination destination = mapResultSetToNotificationDestination(rs);
                    destinations.add(destination);
                }
            }
            
            logger.info("Found {} notification destinations for playground: {}", destinations.size(), playgroundId);
            
        } catch (SQLException e) {
            logger.error("Failed to retrieve notification destinations for playground: {}", playgroundId, e);
            throw e;
        }

        return destinations;
    }

    @Override
    public NotificationDestination getNotificationDestinationById(UUID id) throws SQLException {
        if (id == null) {
            throw new IllegalArgumentException("Notification destination ID cannot be null");
        }

        String sql = "SELECT id, playground_id, destination_type, destination, created_at FROM notification_destinations WHERE id = ?";

        try (Connection conn = metaDBConnection.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setString(1, id.toString());

            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    NotificationDestination destination = mapResultSetToNotificationDestination(rs);
                    logger.info("Found notification destination: {}", id);
                    return destination;
                }
            }
            
            logger.info("Notification destination not found: {}", id);
            
        } catch (SQLException e) {
            logger.error("Failed to retrieve notification destination by ID: {}", id, e);
            throw e;
        }

        return null;
    }

    @Override
    public void deleteNotificationDestinationById(UUID id) throws SQLException {
        if (id == null) {
            throw new IllegalArgumentException("Notification destination ID cannot be null");
        }

        String sql = "DELETE FROM notification_destinations WHERE id = ?";

        try (Connection conn = metaDBConnection.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            
            ps.setString(1, id.toString());

            int rowsDeleted = ps.executeUpdate();
            if (rowsDeleted > 0) {
                logger.info("Deleted notification destination: {}", id);
            } else {
                logger.warn("Notification destination not found for deletion: {}", id);
            }
            
        } catch (SQLException e) {
            logger.error("Failed to delete notification destination: {}", id, e);
            throw e;
        }
    }


    /**
     * Maps a ResultSet row to a NotificationDestination object.
     */
    private NotificationDestination mapResultSetToNotificationDestination(ResultSet rs) throws SQLException {
        NotificationDestination destination = new NotificationDestination();
        destination.setId(UUID.fromString(rs.getString("id")));
        destination.setPlaygroundId(UUID.fromString(rs.getString("playground_id")));
        destination.setDestinationType(rs.getString("destination_type"));
        destination.setDestination(rs.getString("destination"));
        destination.setCreatedAt(rs.getLong("created_at"));
        return destination;
    }
}
