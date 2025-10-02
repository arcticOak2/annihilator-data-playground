package com.annihilator.data.playground.utility;

import com.annihilator.data.playground.cloud.aws.S3Service;
import com.annihilator.data.playground.db.ReconciliationMappingDAO;
import com.annihilator.data.playground.db.ReconciliationResultsDAO;
import com.annihilator.data.playground.db.TaskDAO;
import com.annihilator.data.playground.model.Playground;
import com.annihilator.data.playground.model.Reconciliation;
import com.annihilator.data.playground.model.ReconciliationResultResponse;
import com.annihilator.data.playground.model.Status;
import com.annihilator.data.playground.model.Task;
import com.annihilator.data.playground.model.TaskType;
import com.annihilator.data.playground.notification.NotificationService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.ArgumentMatchers.argThat;

@ExtendWith(MockitoExtension.class)
class DAGNotificationUtilityTest {

    @Mock
    private TaskDAO taskDAO;
    
    @Mock
    private S3Service s3Service;
    
    @Mock
    private NotificationService notificationService;
    
    @Mock
    private ReconciliationMappingDAO reconciliationMappingDAO;
    
    @Mock
    private ReconciliationResultsDAO reconciliationResultsDAO;

    private DAGNotificationUtility dagNotificationUtility;
    private UUID dagId;

    @BeforeEach
    void setUp() {
        dagId = UUID.randomUUID();
        dagNotificationUtility = new DAGNotificationUtility(
                dagId,
                taskDAO,
                s3Service,
                notificationService,
                reconciliationMappingDAO,
                reconciliationResultsDAO
        );
    }

    @Test
    void testSendDAGExecutionNotification_WithSuccessfulTasks() throws Exception {
        // Arrange
        List<Task> tasks = createMockTasks();
        when(taskDAO.findTasksByPlaygroundRecursively(dagId)).thenReturn(tasks);
        when(reconciliationMappingDAO.findReconciliationMappingByPlaygroundId(dagId.toString()))
                .thenReturn(Collections.emptyList());

        // Act
        dagNotificationUtility.sendDAGExecutionNotification("Test Subject", "test@example.com");

        // Assert
        verify(notificationService).notify(eq("Test Subject"), anyString(), eq("test@example.com"));
        verify(taskDAO).findTasksByPlaygroundRecursively(dagId);
    }

    @Test
    void testSendDAGExecutionNotification_WithReconciliation() throws Exception {
        // Arrange
        List<Task> tasks = createMockTasks();
        List<Reconciliation> reconciliations = createMockReconciliations();
        ReconciliationResultResponse reconciliationResult = createMockReconciliationResult();

        when(taskDAO.findTasksByPlaygroundRecursively(dagId)).thenReturn(tasks);
        when(reconciliationMappingDAO.findReconciliationMappingByPlaygroundId(dagId.toString()))
                .thenReturn(reconciliations);
        when(reconciliationResultsDAO.getReconciliationResult(anyString()))
                .thenReturn(reconciliationResult);
        when(taskDAO.findTaskById(anyString())).thenReturn(createMockTask("Test Task", Status.SUCCESS));

        // Act
        dagNotificationUtility.sendDAGExecutionNotification("Test Subject", "test@example.com");

        // Assert
        verify(notificationService).notify(eq("Test Subject"), anyString(), eq("test@example.com"));
        verify(reconciliationResultsDAO).getReconciliationResult(anyString());
    }

    @Test
    void testSendDAGExecutionNotification_WithMixedTaskStatuses() throws Exception {
        // Arrange
        List<Task> tasks = createMixedStatusTasks();
        when(taskDAO.findTasksByPlaygroundRecursively(dagId)).thenReturn(tasks);
        when(reconciliationMappingDAO.findReconciliationMappingByPlaygroundId(dagId.toString()))
                .thenReturn(Collections.emptyList());

        // Act
        dagNotificationUtility.sendDAGExecutionNotification("Test Subject", "test@example.com");

        // Assert
        verify(notificationService).notify(eq("Test Subject"), anyString(), eq("test@example.com"));
        
        // Verify the email content contains all status types
        verify(notificationService).notify(eq("Test Subject"), 
                argThat(content -> content.contains("SUCCESS") && 
                                 content.contains("FAILED") && 
                                 content.contains("SKIPPED") &&
                                 content.contains("CANCELLED") &&
                                 content.contains("UPSTREAM_FAILED")), 
                eq("test@example.com"));
    }

    @Test
    void testSendDAGExecutionNotification_WithEmptyTasks() throws Exception {
        // Arrange
        when(taskDAO.findTasksByPlaygroundRecursively(dagId)).thenReturn(Collections.emptyList());
        when(reconciliationMappingDAO.findReconciliationMappingByPlaygroundId(dagId.toString()))
                .thenReturn(Collections.emptyList());

        // Act
        dagNotificationUtility.sendDAGExecutionNotification("Test Subject", "test@example.com");

        // Assert
        verify(notificationService).notify(eq("Test Subject"), 
                argThat(content -> content.contains("0") && content.contains("Total Tasks")), 
                eq("test@example.com"));
    }

    @Test
    void testSendDAGExecutionNotification_WithFailedReconciliation() throws Exception {
        // Arrange
        List<Task> tasks = createMockTasks();
        List<Reconciliation> reconciliations = createMockReconciliations();
        ReconciliationResultResponse failedResult = createFailedReconciliationResult();

        when(taskDAO.findTasksByPlaygroundRecursively(dagId)).thenReturn(tasks);
        when(reconciliationMappingDAO.findReconciliationMappingByPlaygroundId(dagId.toString()))
                .thenReturn(reconciliations);
        when(reconciliationResultsDAO.getReconciliationResult(anyString()))
                .thenReturn(failedResult);
        when(taskDAO.findTaskById(anyString())).thenReturn(createMockTask("Test Task", Status.SUCCESS));

        // Act
        dagNotificationUtility.sendDAGExecutionNotification("Test Subject", "test@example.com");

        // Assert
        verify(notificationService).notify(eq("Test Subject"), 
                argThat(content -> content.contains("FAILED") && content.contains("status-failed")), 
                eq("test@example.com"));
    }

    @Test
    void testSendDAGExecutionNotification_WithException() throws Exception {
        // Arrange
        when(taskDAO.findTasksByPlaygroundRecursively(dagId)).thenThrow(new RuntimeException("Database error"));

        // Act & Assert
        assertThrows(NotificationException.class, () -> 
                dagNotificationUtility.sendDAGExecutionNotification("Test Subject", "test@example.com"));
    }

    @Test
    void testEmailContentContainsRequiredElements() throws Exception {
        // Arrange
        List<Task> tasks = createMockTasks();
        when(taskDAO.findTasksByPlaygroundRecursively(dagId)).thenReturn(tasks);
        when(reconciliationMappingDAO.findReconciliationMappingByPlaygroundId(dagId.toString()))
                .thenReturn(Collections.emptyList());

        // Act
        dagNotificationUtility.sendDAGExecutionNotification("Test Subject", "test@example.com");

        // Assert
        verify(notificationService).notify(eq("Test Subject"), 
                argThat(content -> {
                    // Check for required HTML structure
                    return content.contains("<html>") &&
                           content.contains("DAG Execution Report") &&
                           content.contains("DAG ID:") &&
                           content.contains("Total Tasks") &&
                           content.contains("Task Execution Results") &&
                           content.contains("<table>") &&
                           content.contains("Step Name") &&
                           content.contains("Status") &&
                           content.contains("</html>");
                }), 
                eq("test@example.com"));
    }

    @Test
    void testNumberFormatting() throws Exception {
        // Arrange
        List<Task> tasks = createMockTasks();
        List<Reconciliation> reconciliations = createMockReconciliations();
        ReconciliationResultResponse result = createLargeNumberReconciliationResult();

        when(taskDAO.findTasksByPlaygroundRecursively(dagId)).thenReturn(tasks);
        when(reconciliationMappingDAO.findReconciliationMappingByPlaygroundId(dagId.toString()))
                .thenReturn(reconciliations);
        when(reconciliationResultsDAO.getReconciliationResult(anyString()))
                .thenReturn(result);
        when(taskDAO.findTaskById(anyString())).thenReturn(createMockTask("Test Task", Status.SUCCESS));

        // Act
        dagNotificationUtility.sendDAGExecutionNotification("Test Subject", "test@example.com");

        // Assert
        verify(notificationService).notify(eq("Test Subject"), 
                argThat(content -> content.contains("1,234,567")), // Check comma formatting
                eq("test@example.com"));
    }

    // Helper methods to create mock objects

    private List<Task> createMockTasks() {
        return Arrays.asList(
                createMockTask("Task 1", Status.SUCCESS),
                createMockTask("Task 2", Status.SUCCESS),
                createMockTask("Task 3", Status.SKIPPED)
        );
    }

    private List<Task> createMixedStatusTasks() {
        return Arrays.asList(
                createMockTask("Success Task", Status.SUCCESS),
                createMockTask("Failed Task", Status.FAILED),
                createMockTask("Skipped Task", Status.SKIPPED),
                createMockTask("Cancelled Task", Status.CANCELLED),
                createMockTask("Upstream Failed Task", Status.UPSTREAM_FAILED)
        );
    }

    private Task createMockTask(String name, Status lastRunStatus) {
        Task task = new Task();
        task.setId(UUID.randomUUID());
        task.setName(name);
        task.setLastRunStatus(lastRunStatus);
        task.setType(TaskType.SQL);
        return task;
    }

    private List<Reconciliation> createMockReconciliations() {
        Reconciliation reconciliation = new Reconciliation();
        reconciliation.setReconciliationId(UUID.randomUUID());
        reconciliation.setPlaygroundId(dagId.toString());
        reconciliation.setLeftTableId(UUID.randomUUID().toString());
        reconciliation.setRightTableId(UUID.randomUUID().toString());
        return Arrays.asList(reconciliation);
    }

    private ReconciliationResultResponse createMockReconciliationResult() {
        ReconciliationResultResponse result = new ReconciliationResultResponse();
        result.setStatus("SUCCESS");
        result.setReconciliationMethod("PROBABILISTIC");
        result.setLeftFileRowCount(1000);
        result.setRightFileRowCount(950);
        result.setCommonRowCount(900);
        result.setLeftFileExclusiveRowCount(100);
        result.setRightFileExclusiveRowCount(50);
        result.setExecutionTimestamp(new Timestamp(System.currentTimeMillis()));
        return result;
    }

    private ReconciliationResultResponse createFailedReconciliationResult() {
        ReconciliationResultResponse result = new ReconciliationResultResponse();
        result.setStatus("FAILED");
        result.setReconciliationMethod("EXACT_MATCH");
        result.setLeftFileRowCount(1000);
        result.setRightFileRowCount(0);
        result.setCommonRowCount(0);
        result.setLeftFileExclusiveRowCount(1000);
        result.setRightFileExclusiveRowCount(0);
        result.setExecutionTimestamp(new Timestamp(System.currentTimeMillis()));
        return result;
    }

    private ReconciliationResultResponse createLargeNumberReconciliationResult() {
        ReconciliationResultResponse result = new ReconciliationResultResponse();
        result.setStatus("SUCCESS");
        result.setReconciliationMethod("PROBABILISTIC");
        result.setLeftFileRowCount(1234567);
        result.setRightFileRowCount(1234567);
        result.setCommonRowCount(1234567);
        result.setLeftFileExclusiveRowCount(0);
        result.setRightFileExclusiveRowCount(0);
        result.setExecutionTimestamp(new Timestamp(System.currentTimeMillis()));
        return result;
    }
}
