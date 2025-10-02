package com.annihilator.data.playground.db;

import com.annihilator.data.playground.model.NotificationDestination;

import java.sql.SQLException;
import java.util.List;
import java.util.UUID;

public interface NotificationDestinationDAO {

    /**
     * Creates a new notification destination.
     * 
     * @param destination The notification destination to create
     * @throws SQLException if database operation fails
     */
    void createNotificationDestination(NotificationDestination destination) throws SQLException;

    /**
     * Retrieves all notification destinations for a specific playground.
     * 
     * @param playgroundId The playground ID
     * @return List of notification destinations
     * @throws SQLException if database operation fails
     */
    List<NotificationDestination> getNotificationDestinationsByPlaygroundId(UUID playgroundId) throws SQLException;

    /**
     * Retrieves a specific notification destination by ID.
     * 
     * @param id The notification destination ID
     * @return The notification destination or null if not found
     * @throws SQLException if database operation fails
     */
    NotificationDestination getNotificationDestinationById(UUID id) throws SQLException;

    /**
     * Deletes a notification destination by ID.
     * 
     * @param id The notification destination ID to delete
     * @throws SQLException if database operation fails
     */
    void deleteNotificationDestinationById(UUID id) throws SQLException;
}
