package com.annihilator.data.playground.db;

import com.annihilator.data.playground.model.Task;
import com.annihilator.data.playground.model.Status;

import java.sql.SQLException;
import java.util.List;
import java.util.UUID;

public interface TaskDAO {

    Task findTaskById(String taskId) throws SQLException;

    void updateTaskStatus(UUID taskId, UUID correlationId, Status status) throws SQLException;

    List<Task> findTasksByPlaygroundRecursively(UUID playgroundId) throws SQLException;

    void createTaskAndUpdatePlayground(Task task) throws SQLException;

    void updateTaskQueryAndPlayground(UUID taskId, String query, UUID playgroundId, String udfIds) throws SQLException;

    void deleteTaskAndUpdatePlayground(String taskId) throws SQLException;

    void updateTaskCompletion(UUID taskId, Status currentStatus, Status lastRunStatus, String outputPath, String logPath, UUID correlationId) throws SQLException;
}
