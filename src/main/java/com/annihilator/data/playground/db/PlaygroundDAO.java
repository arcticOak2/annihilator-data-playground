package com.annihilator.data.playground.db;

import com.annihilator.data.playground.model.Playground;
import com.annihilator.data.playground.model.Status;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public interface PlaygroundDAO {

    void createPlayground(Playground playground) throws SQLException;

    void deletePlayground(UUID playgroundId) throws SQLException;

    Map<String, Object> getPlaygrounds(String userId) throws SQLException;

    Playground getPlaygroundById(UUID id) throws SQLException;

    Map<String, Playground> getAllPlaygrounds() throws SQLException;

    Map<String, Playground> getPlaygroundsUpdatedOrCreatedAfter(long time) throws SQLException;

    public void updatePlayground(UUID id, String newName, String cron) throws SQLException;

    void updatePlaygroundCompletion(UUID id, Status status, long endTime, int successCount, int failureCount, Status runStatus) throws SQLException;

    void updatePlaygroundStart(UUID id, UUID correlationId, long executedTime, Status status) throws SQLException;

    List<Playground> getAllPlaygroundsByStatus(Status status) throws SQLException;
}
