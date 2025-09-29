// package com.annihilator.data.playground.db;
//
// import com.annihilator.data.playground.model.Playground;
// import com.annihilator.data.playground.model.Status;
// import org.junit.jupiter.api.BeforeEach;
// import org.junit.jupiter.api.Test;
// import org.junit.jupiter.api.extension.ExtendWith;
// import org.mockito.Mock;
// import org.mockito.junit.jupiter.MockitoExtension;
//
// import java.sql.*;
// import java.util.List;
// import java.util.Map;
// import java.util.UUID;
//
// import static org.junit.jupiter.api.Assertions.*;
// import static org.mockito.ArgumentMatchers.*;
// import static org.mockito.Mockito.*;
//
// @ExtendWith(MockitoExtension.class)
// class PlaygroundDAOImplTest {
//
//    @Mock
//    private MetaDBConnection metaDBConnection;
//
//    @Mock
//    private Connection connection;
//
//    @Mock
//    private PreparedStatement preparedStatement;
//
//    @Mock
//    private ResultSet resultSet;
//
//    private PlaygroundDAOImpl playgroundDAO;
//
//    @BeforeEach
//    void setUp() throws SQLException {
//        when(metaDBConnection.getConnection()).thenReturn(connection);
//        playgroundDAO = new PlaygroundDAOImpl(metaDBConnection);
//    }
//
//    @Test
//    void testCreatePlayground_WithValidPlayground_ShouldSucceed() throws SQLException {
//        // Given
//        Playground playground = createPlayground("Test Playground", "0 0 12 * * ?");
//        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
//        when(preparedStatement.executeUpdate()).thenReturn(1);
//
//        // When
//        playgroundDAO.createPlayground(playground);
//
//        // Then
//        verify(connection).prepareStatement(contains("INSERT INTO playgrounds"));
//        verify(preparedStatement).setString(eq(1), anyString()); // UUID
//        verify(preparedStatement).setString(eq(2), eq(playground.getName()));
//        verify(preparedStatement).setLong(eq(3), anyLong()); // created_at
//        verify(preparedStatement).setLong(eq(4), anyLong()); // modified_at
//        verify(preparedStatement).setString(eq(5), eq(playground.getUserId()));
//        verify(preparedStatement).setString(eq(6), eq(playground.getCronExpression()));
//        verify(preparedStatement).executeUpdate();
//        verify(preparedStatement).close();
//        verify(connection).close();
//    }
//
//    @Test
//    void testCreatePlayground_WithSQLException_ShouldThrowException() throws SQLException {
//        // Given
//        Playground playground = createPlayground("Test Playground", "0 0 12 * * ?");
//        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
//        when(preparedStatement.executeUpdate()).thenThrow(new SQLException("Database error"));
//
//        // When & Then
//        assertThrows(SQLException.class, () -> playgroundDAO.createPlayground(playground));
//        verify(preparedStatement).close();
//        verify(connection).close();
//    }
//
//    @Test
//    void testUpdatePlayground_WithValidData_ShouldSucceed() throws SQLException {
//        // Given
//        UUID id = UUID.randomUUID();
//        String newName = "Updated Playground";
//        String cronExpression = "0 0 18 * * ?";
//
//        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
//        when(preparedStatement.executeUpdate()).thenReturn(1);
//
//        // When
//        playgroundDAO.updatePlayground(id, newName, cronExpression);
//
//        // Then
//        verify(connection).prepareStatement(contains("UPDATE playgrounds"));
//        verify(preparedStatement).setString(eq(1), eq(newName));
//        verify(preparedStatement).setLong(eq(2), anyLong()); // modified_at
//        verify(preparedStatement).setString(eq(3), eq(cronExpression));
//        verify(preparedStatement).setString(eq(4), eq(id.toString()));
//        verify(preparedStatement).executeUpdate();
//        verify(preparedStatement).close();
//        verify(connection).close();
//    }
//
//    @Test
//    void testUpdatePlayground_WithSQLException_ShouldThrowException() throws SQLException {
//        // Given
//        UUID id = UUID.randomUUID();
//        String newName = "Updated Playground";
//        String cronExpression = "0 0 18 * * ?";
//
//        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
//        when(preparedStatement.executeUpdate()).thenThrow(new SQLException("Database error"));
//
//        // When & Then
//        assertThrows(SQLException.class, () -> playgroundDAO.updatePlayground(id, newName,
// cronExpression));
//        verify(preparedStatement).close();
//        verify(connection).close();
//    }
//
//    @Test
//    void testGetPlaygroundById_WithValidId_ShouldReturnPlayground() throws SQLException {
//        // Given
//        UUID id = UUID.randomUUID();
//        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
//        when(preparedStatement.executeQuery()).thenReturn(resultSet);
//        when(resultSet.next()).thenReturn(true);
//        when(resultSet.getString("id")).thenReturn(id.toString());
//        when(resultSet.getString("name")).thenReturn("Test Playground");
//        when(resultSet.getString("user_id")).thenReturn("test-user");
//        when(resultSet.getString("cron_expression")).thenReturn("0 0 12 * * ?");
//        when(resultSet.getLong("created_at")).thenReturn(System.currentTimeMillis());
//        when(resultSet.getLong("modified_at")).thenReturn(System.currentTimeMillis());
//        when(resultSet.getLong("last_executed_at")).thenReturn(System.currentTimeMillis());
//        when(resultSet.getString("current_status")).thenReturn("IDLE");
//
// when(resultSet.getString("correlation_id")).thenReturn("550e8400-e29b-41d4-a716-446655440000");
//
//        // When
//        Playground result = playgroundDAO.getPlaygroundById(id);
//
//        // Then
//        assertNotNull(result);
//        assertEquals(id, result.getId());
//        assertEquals("Test Playground", result.getName());
//        assertEquals("test-user", result.getUserId());
//        assertEquals("0 0 12 * * ?", result.getCronExpression());
//        verify(preparedStatement).setString(1, id.toString());
//        verify(preparedStatement).close();
//        verify(connection).close();
//    }
//
//    @Test
//    void testGetPlaygroundById_WithNonExistentId_ShouldReturnNull() throws SQLException {
//        // Given
//        UUID id = UUID.randomUUID();
//        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
//        when(preparedStatement.executeQuery()).thenReturn(resultSet);
//        when(resultSet.next()).thenReturn(false);
//
//        // When
//        Playground result = playgroundDAO.getPlaygroundById(id);
//
//        // Then
//        assertNull(result);
//        verify(preparedStatement).setString(1, id.toString());
//        verify(preparedStatement).close();
//        verify(connection).close();
//    }
//
//    @Test
//    void testGetPlaygroundById_WithSQLException_ShouldThrowException() throws SQLException {
//        // Given
//        UUID id = UUID.randomUUID();
//        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
//        when(preparedStatement.executeQuery()).thenThrow(new SQLException("Database error"));
//
//        // When & Then
//        assertThrows(SQLException.class, () -> playgroundDAO.getPlaygroundById(id));
//        verify(preparedStatement).close();
//        verify(connection).close();
//    }
//
//    @Test
//    void testGetAllPlaygrounds_ShouldReturnAllPlaygrounds() throws SQLException {
//        // Given
//        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
//        when(preparedStatement.executeQuery()).thenReturn(resultSet);
//        when(resultSet.next()).thenReturn(true, true, false);
//
//        // Mock the fields that the method actually uses
//        // The method calls getString("id") twice for each row (once for the object, once for the
// map key)
//        when(resultSet.getString("id")).thenReturn("550e8400-e29b-41d4-a716-446655440000",
// "550e8400-e29b-41d4-a716-446655440000", "550e8400-e29b-41d4-a716-446655440001",
// "550e8400-e29b-41d4-a716-446655440001");
//        when(resultSet.getString("name")).thenReturn("Playground 1", "Playground 2");
//        when(resultSet.getString("user_id")).thenReturn("user1", "user2");
//        when(resultSet.getString("cron_expression")).thenReturn("0 0 12 * * ?", "0 0 18 * * ?");
//        when(resultSet.getLong("created_at")).thenReturn(System.currentTimeMillis(),
// System.currentTimeMillis());
//
//        // When
//        Map<String, Playground> result = playgroundDAO.getAllPlaygrounds();
//
//        // Then
//        assertNotNull(result);
//        assertEquals(2, result.size());
//        verify(preparedStatement).executeQuery();
//        verify(resultSet, times(3)).next();
//        verify(preparedStatement).close();
//        verify(connection).close();
//    }
//
//    @Test
//    void testGetAllPlaygroundsByStatus_WithValidStatus_ShouldReturnPlaygrounds() throws
// SQLException {
//        // Given
//        Status status = Status.RUNNING;
//        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
//        when(preparedStatement.executeQuery()).thenReturn(resultSet);
//        when(resultSet.next()).thenReturn(true, false);
//        when(resultSet.getString("id")).thenReturn("550e8400-e29b-41d4-a716-446655440000");
//        when(resultSet.getString("name")).thenReturn("Running Playground");
//        when(resultSet.getString("user_id")).thenReturn("user1");
//        when(resultSet.getString("cron_expression")).thenReturn("0 0 12 * * ?");
//        when(resultSet.getLong("created_at")).thenReturn(System.currentTimeMillis());
//        when(resultSet.getLong("modified_at")).thenReturn(System.currentTimeMillis());
//
// when(resultSet.getString("correlation_id")).thenReturn("550e8400-e29b-41d4-a716-446655440000");
//
//        // When
//        List<Playground> result = playgroundDAO.getAllPlaygroundsByStatus(status);
//
//        // Then
//        assertNotNull(result);
//        assertEquals(1, result.size());
//        assertEquals("Running Playground", result.get(0).getName());
//        verify(preparedStatement).setString(1, status.name());
//        verify(preparedStatement).close();
//        verify(connection).close();
//    }
//
//    @Test
//    void testGetPlaygroundCurrentStatus_WithValidId_ShouldReturnStatus() throws SQLException {
//        // Given
//        UUID id = UUID.randomUUID();
//        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
//        when(preparedStatement.executeQuery()).thenReturn(resultSet);
//        when(resultSet.next()).thenReturn(true);
//        when(resultSet.getString("current_status")).thenReturn("RUNNING");
//
//        // When
//
//        // Then
//        verify(preparedStatement).setString(1, id.toString());
//        verify(preparedStatement).close();
//        verify(connection).close();
//    }
//
//    @Test
//    void testGetPlaygroundCurrentStatus_WithNonExistentId_ShouldReturnNull() throws SQLException {
//        // Given
//        UUID id = UUID.randomUUID();
//        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
//        when(preparedStatement.executeQuery()).thenReturn(resultSet);
//        when(resultSet.next()).thenReturn(false);
//
//        // When
//
//        // Then
//        verify(preparedStatement).setString(1, id.toString());
//        verify(preparedStatement).close();
//        verify(connection).close();
//    }
//
//    @Test
//    void testGetPlaygrounds_WithValidUserId_ShouldReturnPlaygrounds() throws SQLException {
//        // Given
//        String userId = "test-user";
//        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
//        when(preparedStatement.executeQuery()).thenReturn(resultSet);
//        when(resultSet.next()).thenReturn(true, false);
//        when(resultSet.getString("id")).thenReturn("550e8400-e29b-41d4-a716-446655440000");
//        when(resultSet.getString("name")).thenReturn("User Playground");
//        when(resultSet.getString("user_id")).thenReturn(userId);
//        when(resultSet.getString("cron_expression")).thenReturn("0 0 12 * * ?");
//        when(resultSet.getLong("created_at")).thenReturn(System.currentTimeMillis());
//        when(resultSet.getLong("modified_at")).thenReturn(System.currentTimeMillis());
//
//        // When
//        Map<String, Object> result = playgroundDAO.getPlaygrounds(userId);
//
//        // Then
//        assertNotNull(result);
//        assertEquals(1, result.size());
//        assertTrue(result.containsKey("550e8400-e29b-41d4-a716-446655440000"));
//        verify(preparedStatement).setString(1, userId);
//        verify(preparedStatement).close();
//        verify(connection).close();
//    }
//
//    @Test
//    void testUpdatePlaygroundStart_WithValidData_ShouldSucceed() throws SQLException {
//        // Given
//        UUID id = UUID.randomUUID();
//        UUID correlationId = UUID.randomUUID();
//        long startTime = System.currentTimeMillis();
//        Status status = Status.RUNNING;
//
//        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
//        when(preparedStatement.executeUpdate()).thenReturn(1);
//
//        // When
//        playgroundDAO.updatePlaygroundStart(id, correlationId, startTime, status);
//
//        // Then
//        verify(connection).prepareStatement(anyString());
//        verify(preparedStatement).setLong(1, startTime);
//        verify(preparedStatement).setString(2, correlationId.toString());
//        verify(preparedStatement).setString(3, status.name());
//        verify(preparedStatement).setString(4, id.toString());
//        verify(preparedStatement).executeUpdate();
//        verify(preparedStatement).close();
//        verify(connection).close();
//    }
//
//    @Test
//    void testUpdatePlaygroundCompletion_WithValidData_ShouldSucceed() throws SQLException {
//        // Given
//        UUID id = UUID.randomUUID();
//        Status status = Status.SUCCESS;
//        long endTime = System.currentTimeMillis();
//        int successCount = 5;
//        int failureCount = 1;
//        Status finalStatus = Status.SUCCESS;
//
//        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
//        when(preparedStatement.executeUpdate()).thenReturn(1);
//
//        // When
//        playgroundDAO.updatePlaygroundCompletion(id, status, endTime, successCount, failureCount,
// finalStatus);
//
//        // Then
//        verify(connection).prepareStatement(contains("UPDATE playgrounds"));
//        verify(preparedStatement).setString(1, status.name());
//        verify(preparedStatement).setLong(2, endTime);
//        verify(preparedStatement).setInt(3, successCount);
//        verify(preparedStatement).setInt(4, failureCount);
//        verify(preparedStatement).setString(5, finalStatus.name());
//        verify(preparedStatement).setString(6, id.toString());
//        verify(preparedStatement).executeUpdate();
//        verify(preparedStatement).close();
//        verify(connection).close();
//    }
//
//    // Helper method
//    private Playground createPlayground(String name, String cronExpression) {
//        Playground playground = new Playground();
//        playground.setId(UUID.randomUUID());
//        playground.setName(name);
//        playground.setUserId("test-user");
//        playground.setCronExpression(cronExpression);
//        playground.setCreatedAt(System.currentTimeMillis());
//        playground.setModifiedAt(System.currentTimeMillis());
//        playground.setCurrentStatus(Status.IDLE);
//        return playground;
//    }
// }
