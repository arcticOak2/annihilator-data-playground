package com.annihilator.data.playground.db;

import com.annihilator.data.playground.model.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.*;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class TaskDAOImplTest {

    @Mock
    private MetaDBConnection metaDBConnection;
    
    @Mock
    private Connection connection;
    
    @Mock
    private PreparedStatement preparedStatement;
    
    @Mock
    private ResultSet resultSet;
    
    private TaskDAOImpl taskDAO;

    @BeforeEach
    void setUp() throws SQLException {
        when(metaDBConnection.getConnection()).thenReturn(connection);
        taskDAO = new TaskDAOImpl(metaDBConnection);
    }

    @Test
    void testUpdateTaskStatus_WithValidData_ShouldSucceed() throws SQLException {
        // Given
        UUID taskId = UUID.randomUUID();
        UUID correlationId = UUID.randomUUID();
        Status status = Status.RUNNING;
        
        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
        when(preparedStatement.executeUpdate()).thenReturn(1);

        // When
        taskDAO.updateTaskStatus(taskId, correlationId, status);

        // Then
        verify(connection).prepareStatement(anyString());
        verify(preparedStatement).setString(eq(1), eq(status.name()));
        verify(preparedStatement).setString(eq(2), eq(correlationId.toString()));
        verify(preparedStatement).setLong(eq(3), anyLong());
        verify(preparedStatement).setString(eq(4), eq(taskId.toString()));
        verify(preparedStatement).executeUpdate();
        verify(preparedStatement).close();
        verify(connection).close();
    }

    @Test
    void testUpdateTaskStatus_WithSQLException_ShouldThrowException() throws SQLException {
        // Given
        UUID taskId = UUID.randomUUID();
        UUID correlationId = UUID.randomUUID();
        Status status = Status.RUNNING;
        
        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
        when(preparedStatement.executeUpdate()).thenThrow(new SQLException("Database error"));

        // When & Then
        assertThrows(SQLException.class, () -> taskDAO.updateTaskStatus(taskId, correlationId, status));
        verify(preparedStatement).close();
        verify(connection).close();
    }

    @Test
    void testUpdateTaskCompletion_WithValidData_ShouldSucceed() throws SQLException {
        // Given
        UUID taskId = UUID.randomUUID();
        Status status = Status.SUCCESS;
        Status finalStatus = Status.SUCCESS;
        String outputPath = "s3://bucket/output.txt";
        String logPath = "s3://bucket/log.txt";
        UUID correlationId = UUID.randomUUID();
        
        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
        when(preparedStatement.executeUpdate()).thenReturn(1);

        // When
        taskDAO.updateTaskCompletion(taskId, status, finalStatus, outputPath, logPath, correlationId);

        // Then
        verify(connection).prepareStatement(anyString());
        verify(preparedStatement).setString(eq(1), eq(status.name()));
        verify(preparedStatement).setString(eq(2), eq(finalStatus.name()));
        verify(preparedStatement).setLong(eq(3), anyLong());
        verify(preparedStatement).setString(eq(4), eq(outputPath));
        verify(preparedStatement).setString(eq(5), eq(logPath));
        verify(preparedStatement).setString(eq(6), eq(correlationId.toString()));
        verify(preparedStatement).setString(eq(7), eq(taskId.toString()));
        verify(preparedStatement).executeUpdate();
        verify(preparedStatement).close();
        verify(connection).close();
    }

    @Test
    void testUpdateTaskCompletion_WithSQLException_ShouldThrowException() throws SQLException {
        // Given
        UUID taskId = UUID.randomUUID();
        Status status = Status.SUCCESS;
        Status finalStatus = Status.SUCCESS;
        String outputPath = "s3://bucket/output.txt";
        String logPath = "s3://bucket/log.txt";
        UUID correlationId = UUID.randomUUID();
        
        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
        when(preparedStatement.executeUpdate()).thenThrow(new SQLException("Database error"));

        // When & Then
        assertThrows(SQLException.class, () -> 
            taskDAO.updateTaskCompletion(taskId, status, finalStatus, outputPath, logPath, correlationId));
        verify(preparedStatement).close();
        verify(connection).close();
    }

    @Test
    void testFindTasksByPlaygroundRecursively_WithValidPlaygroundId_ShouldReturnTasks() throws SQLException {
        // Given
        UUID playgroundId = UUID.randomUUID();
        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true, true, false);
        
        // First task
        when(resultSet.getString("id")).thenReturn("550e8400-e29b-41d4-a716-446655440000");
        when(resultSet.getString("name")).thenReturn("Task 1");
        when(resultSet.getString("playground_id")).thenReturn(playgroundId.toString());
        when(resultSet.getString("parent_id")).thenReturn(null);
        when(resultSet.getString("query")).thenReturn("SELECT 1");
        when(resultSet.getString("type")).thenReturn("SPARK_SQL");
        when(resultSet.getString("output_location")).thenReturn("s3://test-bucket/output1");
        when(resultSet.getString("log_path")).thenReturn("s3://test-bucket/logs1");
        when(resultSet.getString("result_preview")).thenReturn("preview1");
        when(resultSet.getString("task_status")).thenReturn("PENDING");
        when(resultSet.getString("correlation_id")).thenReturn("550e8400-e29b-41d4-a716-446655440010");
        when(resultSet.getString("last_correlation_id")).thenReturn("550e8400-e29b-41d4-a716-446655440011");
        when(resultSet.getString("last_run_status")).thenReturn("SUCCESS");
        when(resultSet.getLong("created_at")).thenReturn(System.currentTimeMillis());
        when(resultSet.getLong("modified_at")).thenReturn(System.currentTimeMillis());
        
        // Second task
        when(resultSet.getString("id")).thenReturn("550e8400-e29b-41d4-a716-446655440001");
        when(resultSet.getString("name")).thenReturn("Task 2");
        when(resultSet.getString("playground_id")).thenReturn(playgroundId.toString());
        when(resultSet.getString("parent_id")).thenReturn("550e8400-e29b-41d4-a716-446655440000");
        when(resultSet.getString("query")).thenReturn("SELECT 2");
        when(resultSet.getString("type")).thenReturn("SPARK_SQL");
        when(resultSet.getString("output_location")).thenReturn("s3://test-bucket/output2");
        when(resultSet.getString("log_path")).thenReturn("s3://test-bucket/logs2");
        when(resultSet.getString("result_preview")).thenReturn("preview2");
        when(resultSet.getString("task_status")).thenReturn("PENDING");
        when(resultSet.getString("correlation_id")).thenReturn("550e8400-e29b-41d4-a716-446655440020");
        when(resultSet.getString("last_correlation_id")).thenReturn("550e8400-e29b-41d4-a716-446655440021");
        when(resultSet.getString("last_run_status")).thenReturn("PENDING");
        when(resultSet.getLong("created_at")).thenReturn(System.currentTimeMillis());
        when(resultSet.getLong("modified_at")).thenReturn(System.currentTimeMillis());

        // When
        List<Task> result = taskDAO.findTasksByPlaygroundRecursively(playgroundId);

        // Then
        assertNotNull(result);
        assertEquals(2, result.size());
        verify(preparedStatement).setString(1, playgroundId.toString());
        verify(resultSet, times(3)).next();
        verify(resultSet).close();
        verify(preparedStatement).close();
        verify(connection).close();
    }

    @Test
    void testFindTasksByPlaygroundRecursively_WithNullLastRunStatus_ShouldHandleGracefully() throws SQLException {
        // Given
        UUID playgroundId = UUID.randomUUID();
        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true, false);
        when(resultSet.getString("id")).thenReturn("550e8400-e29b-41d4-a716-446655440000");
        when(resultSet.getString("name")).thenReturn("Task 1");
        when(resultSet.getString("playground_id")).thenReturn(playgroundId.toString());
        when(resultSet.getString("parent_id")).thenReturn(null);
        when(resultSet.getString("query")).thenReturn("SELECT 1");
        when(resultSet.getString("type")).thenReturn("SPARK_SQL");
        when(resultSet.getString("output_location")).thenReturn("s3://test-bucket/output");
        when(resultSet.getString("log_path")).thenReturn("s3://test-bucket/logs");
        when(resultSet.getString("result_preview")).thenReturn("preview");
        when(resultSet.getString("task_status")).thenReturn("PENDING");
        when(resultSet.getString("correlation_id")).thenReturn("550e8400-e29b-41d4-a716-446655440010");
        when(resultSet.getString("last_correlation_id")).thenReturn("550e8400-e29b-41d4-a716-446655440011");
        when(resultSet.getString("last_run_status")).thenReturn(null);
        when(resultSet.getLong("created_at")).thenReturn(System.currentTimeMillis());
        when(resultSet.getLong("modified_at")).thenReturn(System.currentTimeMillis());

        // When
        List<Task> result = taskDAO.findTasksByPlaygroundRecursively(playgroundId);

        // Then
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals(Status.UNKNOWN, result.get(0).getLastRunStatus());
        verify(preparedStatement).setString(1, playgroundId.toString());
        verify(resultSet).close();
        verify(preparedStatement).close();
        verify(connection).close();
    }

    @Test
    void testFindTasksByPlaygroundRecursively_WithSQLException_ShouldThrowException() throws SQLException {
        // Given
        UUID playgroundId = UUID.randomUUID();
        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenThrow(new SQLException("Database error"));

        // When & Then
        assertThrows(SQLException.class, () -> taskDAO.findTasksByPlaygroundRecursively(playgroundId));
        verify(preparedStatement).close();
        verify(connection).close();
    }

    @Test
    void testCreateTaskAndUpdatePlayground_WithValidTask_ShouldSucceed() throws SQLException {
        // Given
        Task task = createTask("Test Task", "SELECT 1");
        UUID playgroundId = UUID.randomUUID();
        
        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
        when(preparedStatement.executeUpdate()).thenReturn(1);

        // When
        taskDAO.createTaskAndUpdatePlayground(task);

        // Then
        verify(connection, times(2)).prepareStatement(anyString()); // One for create task, one for update playground
        verify(preparedStatement, times(2)).executeUpdate();
        verify(preparedStatement, times(2)).close();
        verify(connection).close();
    }

    @Test
    void testCreateTaskAndUpdatePlayground_WithSQLException_ShouldRollback() throws SQLException {
        // Given
        Task task = createTask("Test Task", "SELECT 1");
        UUID playgroundId = UUID.randomUUID();
        
        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
        when(preparedStatement.executeUpdate()).thenThrow(new SQLException("Database error"));

        // When & Then
        assertThrows(SQLException.class, () -> taskDAO.createTaskAndUpdatePlayground(task));
        verify(preparedStatement).close();
        verify(connection).close();
    }

    @Test
    void testUpdateTaskQueryAndPlayground_WithValidData_ShouldSucceed() throws SQLException {
        // Given
        UUID taskId = UUID.randomUUID();
        String newQuery = "SELECT * FROM updated_table";
        UUID playgroundId = UUID.randomUUID();
        
        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
        when(preparedStatement.executeUpdate()).thenReturn(1);

        // When
        taskDAO.updateTaskQueryAndPlayground(taskId, newQuery, playgroundId, null);

        // Then
        verify(connection, times(2)).prepareStatement(anyString()); // One for create task, one for update playground
        verify(preparedStatement, times(2)).executeUpdate();
        verify(preparedStatement, times(2)).close();
        verify(connection).close();
    }

    @Test
    void testUpdateTaskQueryAndPlayground_WithSQLException_ShouldRollback() throws SQLException {
        // Given
        UUID taskId = UUID.randomUUID();
        String newQuery = "SELECT * FROM updated_table";
        UUID playgroundId = UUID.randomUUID();
        
        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
        when(preparedStatement.executeUpdate()).thenThrow(new SQLException("Database error"));

        // When & Then
        assertThrows(SQLException.class, () -> taskDAO.updateTaskQueryAndPlayground(taskId, newQuery, playgroundId, null));
        verify(preparedStatement).close();
        verify(connection).close();
    }

    @Test
    void testDeleteTaskAndUpdatePlayground_WithValidTaskId_ShouldSucceed() throws SQLException {
        // Given
        UUID taskId = UUID.randomUUID();
        UUID playgroundId = UUID.randomUUID();
        
        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getString("playground_id")).thenReturn(playgroundId.toString());
        when(preparedStatement.executeUpdate()).thenReturn(1);

        // When
        taskDAO.deleteTaskAndUpdatePlayground(taskId.toString());

        // Then
        verify(connection, times(3)).prepareStatement(anyString()); // One for get task, one for delete task, one for update playground
        verify(preparedStatement).executeQuery();
        verify(preparedStatement, times(2)).executeUpdate();
        verify(resultSet).close();
        verify(preparedStatement, times(3)).close();
        verify(connection).close();
    }

    @Test
    void testDeleteTaskAndUpdatePlayground_WithSQLException_ShouldRollback() throws SQLException {
        // Given
        UUID taskId = UUID.randomUUID();
        UUID playgroundId = UUID.randomUUID();
        
        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenThrow(new SQLException("Database error"));

        // When & Then
        assertThrows(SQLException.class, () -> taskDAO.deleteTaskAndUpdatePlayground(taskId.toString()));
        verify(preparedStatement).close();
        verify(connection).close();
    }

    // Helper method
    private Task createTask(String name, String query) {
        Task task = new Task();
        task.setId(UUID.randomUUID());
        task.setName(name);
        task.setQuery(query);
        task.setPlaygroundId(UUID.randomUUID());
        task.setType(TaskType.SPARK_SQL);
        task.setCreatedAt(System.currentTimeMillis());
        task.setModifiedAt(System.currentTimeMillis());
        task.setLastRunStatus(Status.PENDING);
        return task;
    }
}