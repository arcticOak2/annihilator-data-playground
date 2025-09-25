package com.annihilator.data.playground.db;

import com.annihilator.data.playground.model.Playground;
import com.annihilator.data.playground.model.Status;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class PlaygroundDAOImpl implements PlaygroundDAO {

    private final MetaDBConnection metaDBConnection;

    public PlaygroundDAOImpl(MetaDBConnection metaDBConnection) {
        this.metaDBConnection = metaDBConnection;
    }

    @Override
    public void createPlayground(Playground playground) throws SQLException {

        String sql = "INSERT INTO playgrounds (id, name, created_at, modified_at, user_id, cron_expression) VALUES (?, ?, ?, ?, ?, ?)";

        UUID id = UUID.randomUUID();
        playground.setId(id);
        try (Connection conn = metaDBConnection.getConnection();
            PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, playground.getId().toString());
            ps.setString(2, playground.getName());
            ps.setLong(3, System.currentTimeMillis());
            ps.setLong(4, System.currentTimeMillis());
            ps.setString(5, playground.getUserId());
            ps.setString(6, playground.getCronExpression());
            ps.executeUpdate();
        } catch (SQLException e) {
            throw e;
        }
    }

    @Override
    public void updatePlayground(UUID id, String newName, String cron) throws SQLException {
        String sql = "UPDATE playgrounds SET name = ?, modified_at = ?, cron_expression = ? WHERE id = ?";
        try (Connection conn = metaDBConnection.getConnection();
            PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, newName);
            ps.setLong(2, System.currentTimeMillis());
            ps.setString(3, cron);
            ps.setString(4, id.toString());
            ps.executeUpdate();
        }
    }

    @Override
    public void updatePlaygroundCompletion(UUID id, Status status, long endTime, int successCount, int failureCount, Status runStatus) throws SQLException {

        String sql = "UPDATE playgrounds SET " +
                "current_status = ?, " +
                "last_run_end_time = ?, " +
                "last_run_success_count = ?, " +
                "last_run_failure_count = ?, " +
                "last_run_status = ? " +
                "WHERE id = ?";

        try (Connection conn = metaDBConnection.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, status.name());
            ps.setLong(2, endTime);
            ps.setInt(3, successCount);
            ps.setInt(4, failureCount);
            ps.setString(5, runStatus.name());
            ps.setString(6, id.toString());
            ps.executeUpdate();
        }
    }

    @Override
    public void updatePlaygroundStart(UUID id, UUID correlationId, long executedTime, Status status) throws SQLException {
        String sql = "UPDATE playgrounds SET " +
                "last_executed_at = ?, " +
                "correlation_id = ?, " +
                "current_status = ? " +
                "WHERE id = ?";

        try (Connection conn = metaDBConnection.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, executedTime);
            ps.setString(2, correlationId.toString());
            ps.setString(3, status.name());
            ps.setString(4, id.toString());
            ps.executeUpdate();
        }
    }

    @Override
    public List<Playground> getAllPlaygroundsByStatus(Status status) throws SQLException {

        String sql = "SELECT id, name, created_at, user_id, modified_at, last_executed_at, cron_expression, correlation_id FROM playgrounds WHERE current_status = ?";
        try (Connection conn = metaDBConnection.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, status.name());
            ResultSet rs = ps.executeQuery();
            List<Playground> playgrounds = new java.util.ArrayList<>();

            while (rs.next()) {

                Playground playground = new Playground();

                playground.setId(UUID.fromString(rs.getString("id")));
                playground.setName(rs.getString("name"));
                playground.setCreatedAt(rs.getLong("created_at"));
                playground.setUserId(rs.getString("user_id"));
                playground.setModifiedAt(rs.getLong("modified_at"));
                playground.setLastExecutedAt(rs.getLong("last_executed_at"));
                playground.setCronExpression(rs.getString("cron_expression"));
                playground.setCorrelationId(rs.getString("correlation_id") != null ? UUID.fromString(rs.getString("correlation_id")) : null);

                playgrounds.add(playground);
            }
            return playgrounds;
        } catch (SQLException e) {
            throw e;
        }
    }


    @Override
    public void deletePlayground(UUID id) throws SQLException {
        String sql = "DELETE FROM playgrounds WHERE id = ?";
        try (Connection conn = metaDBConnection.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, id.toString());
            ps.executeUpdate();
        }
    }

    @Override
    public Map<String, Object> getPlaygrounds(String userId) throws SQLException {

        String sql = "SELECT " +
                "id, " +
                "name, " +
                "created_at, " +
                "user_id, " +
                "modified_at, " +
                "last_executed_at, " +
                "cron_expression, " +
                "current_status, " +
                "last_run_status, " +
                "last_run_end_time, " +
                "last_run_success_count, " +
                "correlation_id, " +
                "last_run_failure_count FROM playgrounds WHERE user_id = ?";

        try (Connection conn = metaDBConnection.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, userId);
            ResultSet rs = ps.executeQuery();
            Map<String, Object> playgrounds = new HashMap<>();
            while (rs.next()) {

                Playground playground = new Playground();

                playground.setId(UUID.fromString(rs.getString("id")));
                playground.setName(rs.getString("name"));
                playground.setCreatedAt(rs.getLong("created_at"));
                playground.setUserId(rs.getString("user_id"));
                playground.setModifiedAt(rs.getLong("modified_at"));
                playground.setLastExecutedAt(rs.getLong("last_executed_at"));
                playground.setCronExpression(rs.getString("cron_expression"));
                playground.setCurrentStatus(rs.getString("current_status") != null ? Status.valueOf(rs.getString("current_status")) : null);
                playground.setLastRunStatus(rs.getString("last_run_status") != null ? Status.valueOf(rs.getString("last_run_status")) : null);
                playground.setLastRunEndTime(rs.getLong("last_run_end_time"));
                playground.setLastRunSuccessCount(rs.getInt("last_run_success_count"));
                playground.setLastRunFailureCount(rs.getInt("last_run_failure_count"));
                playground.setCorrelationId(rs.getString("correlation_id") != null ? UUID.fromString(rs.getString("correlation_id")) : null);

                playgrounds.put(rs.getString("id"), playground);
            }
            return playgrounds;
        } catch (SQLException e) {
            throw e;
        }
    }

    @Override
    public Playground getPlaygroundById(UUID id) throws SQLException {

        String sql = "SELECT id, name, created_at, user_id, modified_at, last_executed_at, cron_expression, correlation_id, current_status FROM playgrounds WHERE id = ?";

        try (Connection conn = metaDBConnection.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, id.toString());
            ResultSet rs = ps.executeQuery();

            if (rs.next()) {

                Playground playground = new Playground();

                playground.setId(UUID.fromString(rs.getString("id")));
                playground.setName(rs.getString("name"));
                playground.setCreatedAt(rs.getLong("created_at"));
                playground.setUserId(rs.getString("user_id"));
                playground.setModifiedAt(rs.getLong("modified_at"));
                playground.setLastExecutedAt(rs.getLong("last_executed_at"));
                playground.setCronExpression(rs.getString("cron_expression"));
                playground.setCurrentStatus(rs.getString("current_status") != null ? Status.valueOf(rs.getString("current_status")) : null);
                playground.setCorrelationId(rs.getString("correlation_id") != null ? UUID.fromString(rs.getString("correlation_id")) : null);

                return playground;
            } else {
                return null;
            }
        } catch (SQLException e) {
            throw e;
        }
    }

    @Override
    public Map<String, Playground> getAllPlaygrounds() throws SQLException {

        String sql = "SELECT id, name, created_at, modified_at, user_id, cron_expression, last_executed_at FROM playgrounds WHERE cron_expression IS NOT NULL";

        try (Connection conn = metaDBConnection.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ResultSet rs = ps.executeQuery();
            Map<String, Playground> playgrounds = new HashMap<>();
            while (rs.next()) {
                Playground playground = new Playground();
                playground.setId(UUID.fromString(rs.getString("id")));
                playground.setName(rs.getString("name"));
                playground.setCreatedAt(rs.getLong("created_at"));
                playground.setUserId(rs.getString("user_id"));
                playground.setCronExpression(rs.getString("cron_expression"));
                playground.setModifiedAt(rs.getLong("modified_at"));
                playground.setLastExecutedAt(rs.getLong("last_executed_at"));

                playgrounds.put(rs.getString("id"), playground);
            }
            return playgrounds;
        } catch (SQLException e) {
            throw e;
        }
    }

    @Override
    public Map<String, Playground> getPlaygroundsUpdatedOrCreatedAfter(long time) throws SQLException {

        String sql = "SELECT id, name, created_at, user_id, modified_at, last_executed_at, cron_expression FROM playgrounds WHERE modified_at > ? OR created_at > ?";

        try (Connection conn = metaDBConnection.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, time);
            ps.setLong(2, time);
            ResultSet rs = ps.executeQuery();
            Map<String, Playground> playgrounds = new HashMap<>();
            while (rs.next()) {
                Playground playground = new Playground();
                playground.setId(UUID.fromString(rs.getString("id")));
                playground.setName(rs.getString("name"));
                playground.setCreatedAt(rs.getLong("created_at"));
                playground.setUserId(rs.getString("user_id"));
                playground.setModifiedAt(rs.getLong("modified_at"));
                playground.setLastExecutedAt(rs.getLong("last_executed_at"));
                playground.setCronExpression(rs.getString("cron_expression"));

                playgrounds.put(rs.getString("id"), playground);
            }
            return playgrounds;
        } catch (SQLException e) {
            throw e;
        }
    }
}
