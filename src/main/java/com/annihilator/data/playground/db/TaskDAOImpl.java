package com.annihilator.data.playground.db;

import com.annihilator.data.playground.model.Status;
import com.annihilator.data.playground.model.Task;
import com.annihilator.data.playground.model.TaskType;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class TaskDAOImpl implements TaskDAO {

  private final MetaDBConnection metaDBConnection;

  public TaskDAOImpl(MetaDBConnection metaDBConnection) {
    this.metaDBConnection = metaDBConnection;
  }

  @Override
  public List<Task> findTasksByPlaygroundRecursively(UUID playgroundId) throws SQLException {
    String sql =
        "SELECT id, name, playground_id, parent_id, created_at, modified_at, type, query, output_location, log_path, result_preview, task_status, last_run_status, correlation_id, last_correlation_id, udf_ids "
            + "FROM tasks "
            + "WHERE playground_id = ? "
            + "ORDER BY created_at ASC";

    List<Task> allTasks = new ArrayList<>();
    Map<UUID, Task> taskMap = new HashMap<>();
    Map<UUID, List<UUID>> childrenMap = new HashMap<>();

    try (Connection conn = metaDBConnection.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {

      ps.setString(1, playgroundId.toString());

      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          Task task = new Task();
          task.setId(UUID.fromString(rs.getString("id")));
          task.setName(rs.getString("name"));
          task.setPlaygroundId(UUID.fromString(rs.getString("playground_id")));

          String parentIdStr = rs.getString("parent_id");
          if (parentIdStr != null && !parentIdStr.isEmpty()) {
            task.setParentId(UUID.fromString(parentIdStr));
          }

          task.setCreatedAt(rs.getLong("created_at"));
          task.setModifiedAt(rs.getLong("modified_at"));
          task.setType(TaskType.valueOf(rs.getString("type")));
          task.setQuery(rs.getString("query"));
          task.setOutputLocation(rs.getString("output_location"));
          task.setLogPath(rs.getString("log_path"));
          task.setResultPreview(rs.getString("result_preview"));
          task.setStatus(Status.valueOf(rs.getString("task_status")));
          task.setCorrelationId(
              rs.getString("correlation_id") != null
                  ? UUID.fromString(rs.getString("correlation_id"))
                  : null);
          task.setLastCorrelationId(
              rs.getString("last_correlation_id") != null
                  ? UUID.fromString(rs.getString("last_correlation_id"))
                  : null);
          if (rs.getString("last_run_status") == null) {
            task.setLastRunStatus(Status.UNKNOWN);
          } else {
            task.setLastRunStatus(Status.valueOf(String.valueOf(rs.getString("last_run_status"))));
          }
          task.setUdfIds(rs.getString("udf_ids"));
          allTasks.add(task);
          taskMap.put(task.getId(), task);

          if (task.getParentId() != null) {
            childrenMap
                .computeIfAbsent(task.getParentId(), k -> new ArrayList<>())
                .add(task.getId());
          }
        }
      }
    }

    return allTasks;
  }

  @Override
  public Task findTaskById(String taskId) throws SQLException {
    String sql =
        "SELECT id, name, playground_id, parent_id, created_at, modified_at, type, query, output_location, log_path, result_preview, task_status, last_run_status, correlation_id, last_correlation_id, udf_ids "
            + "FROM tasks "
            + "WHERE id = ?";

    try (Connection conn = metaDBConnection.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {

      ps.setString(1, taskId);

      try (ResultSet rs = ps.executeQuery()) {

        if (rs.next()) {
          Task task = new Task();
          task.setId(UUID.fromString(rs.getString("id")));
          task.setName(rs.getString("name"));
          task.setPlaygroundId(UUID.fromString(rs.getString("playground_id")));

          String parentIdStr = rs.getString("parent_id");
          if (parentIdStr != null && !parentIdStr.isEmpty()) {
            task.setParentId(UUID.fromString(parentIdStr));
          }

          task.setCreatedAt(rs.getLong("created_at"));
          task.setModifiedAt(rs.getLong("modified_at"));
          task.setType(TaskType.valueOf(rs.getString("type")));
          task.setQuery(rs.getString("query"));
          task.setOutputLocation(rs.getString("output_location"));
          task.setLogPath(rs.getString("log_path"));
          task.setResultPreview(rs.getString("result_preview"));
          task.setStatus(Status.valueOf(rs.getString("task_status")));
          task.setCorrelationId(
              rs.getString("correlation_id") != null
                  ? UUID.fromString(rs.getString("correlation_id"))
                  : null);
          task.setLastCorrelationId(
              rs.getString("last_correlation_id") != null
                  ? UUID.fromString(rs.getString("last_correlation_id"))
                  : null);
          if (rs.getString("last_run_status") == null) {
            task.setLastRunStatus(Status.UNKNOWN);
          } else {
            task.setLastRunStatus(Status.valueOf(String.valueOf(rs.getString("last_run_status"))));
          }
          task.setUdfIds(rs.getString("udf_ids"));
          return task;
        } else {
          return null;
        }
      }
    }
  }

  @Override
  public void updateTaskStatus(UUID taskId, UUID correlationId, Status newStatus)
      throws SQLException {
    String sql =
        "UPDATE tasks SET task_status = ?, correlation_id = ?, modified_at = ? WHERE id = ?";
    try (Connection conn = metaDBConnection.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, newStatus.name());
      ps.setString(2, correlationId.toString());
      ps.setLong(3, System.currentTimeMillis());
      ps.setString(4, taskId.toString());
      ps.executeUpdate();
    }
  }

  @Override
  public void createTaskAndUpdatePlayground(Task task) throws SQLException {
    Connection conn = null;
    try {
      conn = metaDBConnection.getConnection();
      conn.setAutoCommit(false);

      String createTaskSql =
          "INSERT INTO tasks (id, name, playground_id, parent_id, created_at, modified_at, type, query, output_location, log_path, result_preview, task_status, last_run_status, correlation_id, last_correlation_id, udf_ids) "
              + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

      try (PreparedStatement ps = conn.prepareStatement(createTaskSql)) {
        UUID uuid = UUID.randomUUID();
        task.setId(uuid);

        ps.setString(1, uuid.toString());
        ps.setString(2, task.getName());
        ps.setString(3, task.getPlaygroundId().toString());
        ps.setString(4, task.getParentId() != null ? task.getParentId().toString() : null);
        ps.setLong(5, System.currentTimeMillis());
        ps.setLong(6, System.currentTimeMillis());
        ps.setString(7, task.getType().name());
        ps.setString(8, task.getQuery());
        ps.setString(9, null);
        ps.setString(10, null);
        ps.setString(11, task.getResultPreview());
        ps.setString(12, Status.IDLE.name());
        ps.setString(13, Status.UNKNOWN.name());
        ps.setString(14, null);
        ps.setString(15, null);
        ps.setString(16, task.getUdfIds());

        ps.executeUpdate();
      }

      String updatePlaygroundSql = "UPDATE playgrounds SET modified_at = ? WHERE id = ?";
      try (PreparedStatement ps = conn.prepareStatement(updatePlaygroundSql)) {
        ps.setLong(1, System.currentTimeMillis());
        ps.setString(2, task.getPlaygroundId().toString());
        ps.executeUpdate();
      }

      conn.commit();
    } catch (SQLException e) {
      if (conn != null) {
        conn.rollback();
      }
      throw e;
    } finally {
      if (conn != null) {
        conn.setAutoCommit(true);
        conn.close();
      }
    }
  }

  @Override
  public void updateTaskQueryAndPlayground(
      UUID taskId, String query, UUID playgroundId, String udfIds) throws SQLException {
    Connection conn = null;
    try {
      conn = metaDBConnection.getConnection();
      conn.setAutoCommit(false);

      String updateTaskSql =
          "UPDATE tasks SET query = ?, modified_at = ?, udf_ids = ? WHERE id = ?";
      try (PreparedStatement ps = conn.prepareStatement(updateTaskSql)) {
        ps.setString(1, query);
        ps.setLong(2, System.currentTimeMillis());
        ps.setString(3, udfIds);
        ps.setString(4, taskId.toString());
        ps.executeUpdate();
      }

      String updatePlaygroundSql = "UPDATE playgrounds SET modified_at = ? WHERE id = ?";
      try (PreparedStatement ps = conn.prepareStatement(updatePlaygroundSql)) {
        ps.setLong(1, System.currentTimeMillis());
        ps.setString(2, playgroundId.toString());
        ps.executeUpdate();
      }

      conn.commit();
    } catch (SQLException e) {
      if (conn != null) {
        conn.rollback();
      }
      throw e;
    } finally {
      if (conn != null) {
        conn.setAutoCommit(true);
        conn.close();
      }
    }
  }

  @Override
  public void deleteTaskAndUpdatePlayground(String taskId) throws SQLException {
    Connection conn = null;
    try {
      conn = metaDBConnection.getConnection();
      conn.setAutoCommit(false);

      String getTaskSql = "SELECT playground_id FROM tasks WHERE id = ?";
      UUID playgroundId = null;

      try (PreparedStatement ps = conn.prepareStatement(getTaskSql)) {
        ps.setString(1, taskId.toString());
        try (ResultSet rs = ps.executeQuery()) {
          if (rs.next()) {
            playgroundId = UUID.fromString(rs.getString("playground_id"));
          } else {
            throw new SQLException("Task not found with ID: " + taskId);
          }
        }
      }

      String deleteTaskSql = "DELETE FROM tasks WHERE id = ?";
      try (PreparedStatement ps = conn.prepareStatement(deleteTaskSql)) {
        ps.setString(1, taskId);
        int rowsAffected = ps.executeUpdate();
        if (rowsAffected == 0) {
          throw new SQLException("No task deleted with ID: " + taskId);
        }
      }

      String updatePlaygroundSql = "UPDATE playgrounds SET modified_at = ? WHERE id = ?";
      try (PreparedStatement ps = conn.prepareStatement(updatePlaygroundSql)) {
        ps.setLong(1, System.currentTimeMillis());
        ps.setString(2, playgroundId.toString());
        ps.executeUpdate();
      }

      conn.commit();
    } catch (SQLException e) {
      if (conn != null) {
        conn.rollback();
      }
      throw e;
    } finally {
      if (conn != null) {
        conn.setAutoCommit(true);
        conn.close();
      }
    }
  }

  @Override
  public void updateTaskCompletion(
      UUID taskId,
      Status currentStatus,
      Status lastRunStatus,
      String outputPath,
      String logPath,
      UUID correlationId)
      throws SQLException {

    String sql =
        Status.SKIPPED != lastRunStatus
            ? "UPDATE tasks SET task_status = ?, last_run_status = ?, modified_at = ?, output_location = ?, log_path = ?, last_correlation_id = ? WHERE id = ?"
            : "UPDATE tasks SET task_status = ?, last_run_status = ?, modified_at = ?, last_correlation_id = ? WHERE id = ?";

    try (Connection conn = metaDBConnection.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {

      if (Status.SKIPPED != lastRunStatus) {
        ps.setString(1, currentStatus.name());
        ps.setString(2, lastRunStatus.name());
        ps.setLong(3, System.currentTimeMillis());
        ps.setString(4, outputPath);
        ps.setString(5, logPath);
        ps.setString(6, correlationId.toString());
        ps.setString(7, taskId.toString());
      } else {
        ps.setString(1, currentStatus.name());
        ps.setString(2, lastRunStatus.name());
        ps.setLong(3, System.currentTimeMillis());
        ps.setString(4, correlationId.toString());
        ps.setString(5, taskId.toString());
      }

      ps.executeUpdate();
    }
  }
}
