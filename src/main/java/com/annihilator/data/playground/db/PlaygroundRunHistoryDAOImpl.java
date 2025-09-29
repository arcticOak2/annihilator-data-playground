package com.annihilator.data.playground.db;

import com.annihilator.data.playground.model.Status;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PlaygroundRunHistoryDAOImpl implements PlaygroundRunHistoryDAO {

  private static final Logger logger = LoggerFactory.getLogger(PlaygroundRunHistoryDAOImpl.class);

  private final MetaDBConnection metaDBConnection;

  public PlaygroundRunHistoryDAOImpl(MetaDBConnection metaDBConnection) {
    this.metaDBConnection = metaDBConnection;
  }

  @Override
  public void completePlaygroundAndInsertHistory(
      UUID playgroundId,
      Status currentStatus,
      long endTime,
      int successCount,
      int failureCount,
      Status finalStatus)
      throws SQLException {

    Connection conn = null;
    try {
      conn = metaDBConnection.getConnection();

      conn.setAutoCommit(false);

      String updatePlaygroundSql =
          "UPDATE playgrounds SET "
              + "current_status = ?, "
              + "last_run_end_time = ?, "
              + "last_run_success_count = ?, "
              + "last_run_failure_count = ?, "
              + "last_run_status = ? "
              + "WHERE id = ?";

      try (PreparedStatement ps = conn.prepareStatement(updatePlaygroundSql)) {
        ps.setString(1, currentStatus.name());
        ps.setLong(2, endTime);
        ps.setInt(3, successCount);
        ps.setInt(4, failureCount);
        ps.setString(5, finalStatus.name());
        ps.setString(6, playgroundId.toString());

        int playgroundRowsUpdated = ps.executeUpdate();
        if (playgroundRowsUpdated == 0) {
          throw new SQLException("No playground found with ID: " + playgroundId);
        }
        logger.debug("Updated playground completion for playground {}", playgroundId);
      }

      String insertHistorySql =
          "INSERT INTO playground_run_history "
              + "(run_id, child_id, playground_id, playground_name, parent_id, parent_name, "
              + "child_name, task_status, task_type, playground_started_at, playground_ended_at) "
              + "WITH parent_task_cte AS ( "
              + "  SELECT "
              + "    playground.correlation_id as run_id, "
              + "    tasks.playground_id as playground_id, "
              + "    playground_name, "
              + "    id AS task_id, "
              + "    parent_id as parent_task_id, "
              + "    name AS task_name, "
              + "    last_run_status as task_status, "
              + "    start_time, "
              + "    end_time "
              + "  FROM "
              + "    tasks "
              + "    LEFT JOIN ( "
              + "      SELECT "
              + "        id AS playground_id, "
              + "        name AS playground_name, "
              + "        correlation_id, "
              + "        last_run_status AS playground_status, "
              + "        last_executed_at AS start_time, "
              + "        last_run_end_time AS end_time "
              + "      FROM "
              + "        playgrounds "
              + "    ) playground ON tasks.playground_id = playground.playground_id "
              + "  WHERE "
              + "    playground.correlation_id IS NOT NULL "
              + "    AND tasks.playground_id = ? "
              + "    AND playground.start_time IS NOT NULL "
              + "    AND tasks.last_run_status IS NOT NULL "
              + "    AND playground.playground_status IS NOT NULL "
              + ") "
              + "SELECT "
              + "  t1.run_id as run_id, "
              + "  t1.task_id AS child_id, "
              + "  t1.playground_id, "
              + "  t1.playground_name, "
              + "  t2.task_id AS parent_id, "
              + "  t2.task_name AS parent_name, "
              + "  t1.task_name AS child_name, "
              + "  t1.task_status, "
              + "  CASE "
              + "    WHEN t1.parent_task_id IS NULL THEN 'ROOT' "
              + "    ELSE 'CHILD' "
              + "  END as task_type, "
              + "  CASE "
              + "    WHEN t1.start_time IS NOT NULL THEN FROM_UNIXTIME(t1.start_time/1000) "
              + "    ELSE NULL "
              + "  END as playground_started_at, "
              + "  CASE "
              + "    WHEN t1.end_time IS NOT NULL THEN FROM_UNIXTIME(t1.end_time/1000) "
              + "    ELSE NULL "
              + "  END as playground_ended_at "
              + "FROM "
              + "  parent_task_cte AS t1 "
              + "  LEFT JOIN parent_task_cte AS t2 ON t1.parent_task_id = t2.task_id "
              + "ORDER BY "
              + "  t1.playground_id, "
              + "  COALESCE(t2.task_name, t1.task_name), "
              + "  t1.task_name";

      try (PreparedStatement ps = conn.prepareStatement(insertHistorySql)) {
        ps.setString(1, playgroundId.toString());
        int historyRowsInserted = ps.executeUpdate();

        if (historyRowsInserted == 0) {
          logger.warn(
              "No run history inserted for playground {} - no tasks found or invalid data",
              playgroundId);
        } else {
          logger.debug(
              "Inserted {} run history records for playground {}",
              historyRowsInserted,
              playgroundId);
        }
      }

      conn.commit();
      logger.info("Successfully completed playground {} and inserted run history", playgroundId);

    } catch (SQLException e) {
      if (conn != null) {
        conn.rollback();
      }
      logger.error(
          "Failed to complete playground and insert run history for playground {}: {}",
          playgroundId,
          e.getMessage(),
          e);
      throw e;
    } finally {
      if (conn != null) {
        conn.setAutoCommit(true);
        conn.close();
      }
    }
  }

  @Override
  public Map<String, Object> getRunHistoryByPlaygroundId(UUID playgroundId, int limit)
      throws SQLException {
    // First, get the distinct run_ids with their metadata, limited by number of runs
    String runIdsSql =
        "SELECT DISTINCT run_id, playground_name, playground_started_at, playground_ended_at "
            + "FROM playground_run_history "
            + "WHERE playground_id = ? "
            + "ORDER BY playground_started_at DESC "
            + "LIMIT ?";

    try (Connection conn = metaDBConnection.getConnection();
        PreparedStatement ps = conn.prepareStatement(runIdsSql)) {

      ps.setString(1, playgroundId.toString());
      ps.setInt(2, limit);
      ResultSet rs = ps.executeQuery();

      Map<String, Object> result = new HashMap<>();
      List<Map<String, Object>> runs = new ArrayList<>();

      while (rs.next()) {
        String runId = rs.getString("run_id");

        // Get all tasks for this run_id
        List<Map<String, Object>> tasks = getTasksForRunId(conn, runId);

        Map<String, Object> run = new HashMap<>();
        run.put("run_id", runId);
        run.put("playground_name", rs.getString("playground_name"));
        run.put("playground_started_at", rs.getTimestamp("playground_started_at"));
        run.put("playground_ended_at", rs.getTimestamp("playground_ended_at"));
        run.put("tasks", tasks);
        run.put("task_count", tasks.size());

        runs.add(run);
      }

      result.put("playground_id", playgroundId.toString());
      result.put("runs", runs);
      result.put("run_count", runs.size());
      result.put("limit", limit);
      return result;
    }
  }

  private List<Map<String, Object>> getTasksForRunId(Connection conn, String runId)
      throws SQLException {
    String tasksSql =
        "SELECT child_id, parent_id, parent_name, child_name, task_status, task_type "
            + "FROM playground_run_history "
            + "WHERE run_id = ? "
            + "ORDER BY COALESCE(parent_name, child_name), child_name";

    try (PreparedStatement ps = conn.prepareStatement(tasksSql)) {
      ps.setString(1, runId);
      ResultSet rs = ps.executeQuery();

      // Build a map of all tasks first
      Map<String, Map<String, Object>> taskMap = new HashMap<>();
      Map<String, List<Map<String, Object>>> childrenMap = new HashMap<>();

      while (rs.next()) {
        String childId = rs.getString("child_id");
        String parentId = rs.getString("parent_id");

        Map<String, Object> task = new HashMap<>();
        task.put("child_id", childId);
        task.put("parent_id", parentId);
        task.put("parent_name", rs.getString("parent_name"));
        task.put("child_name", rs.getString("child_name"));
        task.put("task_status", rs.getString("task_status"));
        task.put("task_type", rs.getString("task_type"));
        task.put("children", new ArrayList<Map<String, Object>>());

        taskMap.put(childId, task);

        // Build children map
        if (parentId != null) {
          childrenMap.computeIfAbsent(parentId, k -> new ArrayList<>()).add(task);
        }
      }

      // Build hierarchical structure
      List<Map<String, Object>> rootTasks = new ArrayList<>();
      for (Map<String, Object> task : taskMap.values()) {
        String parentId = (String) task.get("parent_id");
        if (parentId == null) {
          // This is a root task
          buildTaskHierarchy(task, childrenMap);
          rootTasks.add(task);
        }
      }

      return rootTasks;
    }
  }

  private void buildTaskHierarchy(
      Map<String, Object> parentTask, Map<String, List<Map<String, Object>>> childrenMap) {
    String childId = (String) parentTask.get("child_id");
    List<Map<String, Object>> children = childrenMap.get(childId);

    if (children != null && !children.isEmpty()) {
      List<Map<String, Object>> taskChildren =
          (List<Map<String, Object>>) parentTask.get("children");
      for (Map<String, Object> child : children) {
        buildTaskHierarchy(child, childrenMap);
        taskChildren.add(child);
      }
    }
  }
}
