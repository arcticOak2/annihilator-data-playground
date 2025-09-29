// package com.annihilator.data.playground.core;
//
// import com.annihilator.data.playground.cloud.aws.EMRService;
// import com.annihilator.data.playground.cloud.aws.EMRServiceImpl;
// import com.annihilator.data.playground.db.PlaygroundDAO;
// import com.annihilator.data.playground.db.TaskDAO;
// import com.annihilator.data.playground.model.*;
// import org.junit.jupiter.api.BeforeEach;
// import org.junit.jupiter.api.Test;
// import org.junit.jupiter.api.extension.ExtendWith;
// import org.mockito.Mock;
// import org.mockito.junit.jupiter.MockitoExtension;
// import software.amazon.awssdk.services.emr.model.StepState;
//
// import java.sql.SQLException;
// import java.util.*;
//
// import static org.junit.jupiter.api.Assertions.*;
// import static org.mockito.ArgumentMatchers.*;
// import static org.mockito.Mockito.*;
//
// @ExtendWith(MockitoExtension.class)
// class DataPhantomPlaygroundExecutorTest {
//
//    @Mock
//    private PlaygroundDAO playgroundDAO;
//
//    @Mock
//    private TaskDAO taskDAO;
//
//    @Mock
//    private EMRService emrService;
//
//    private Playground playground;
//    private Set<String> cancelPlaygroundRequestSet;
//
//    @BeforeEach
//    void setUp() {
//        playground = new Playground();
//        playground.setId(UUID.randomUUID());
//        playground.setName("Test Playground");
//        playground.setUserId("test-user");
//        playground.setCronExpression("0 0 12 * * ?");
//
//        cancelPlaygroundRequestSet = Collections.synchronizedSet(new HashSet<>());
//    }
//
//    @Test
//    void testConstructor_WithValidParameters_ShouldCreateInstance() {
//        // When
//        DataPhantomPlaygroundExecutor executor = new DataPhantomPlaygroundExecutor(
//            playground, taskDAO, playgroundDAO, null, emrService,
//            PlaygroundExecutionType.AD_HOC, cancelPlaygroundRequestSet, false, null
//        );
//
//        // Then
//        assertNotNull(executor);
//    }
//
//    @Test
//    void testConstructor_WithRecoveryType_ShouldCreateInstance() {
//        // When
//        DataPhantomPlaygroundExecutor executor = new DataPhantomPlaygroundExecutor(
//            playground, taskDAO, playgroundDAO, null, emrService,
//            PlaygroundExecutionType.RECOVERY, cancelPlaygroundRequestSet, false, null
//        );
//
//        // Then
//        assertNotNull(executor);
//    }
//
//    @Test
//    void testConstructor_WithScheduledType_ShouldCreateInstance() {
//        // When
//        DataPhantomPlaygroundExecutor executor = new DataPhantomPlaygroundExecutor(
//            playground, taskDAO, playgroundDAO,null, emrService,
//            PlaygroundExecutionType.SCHEDULED, cancelPlaygroundRequestSet, false, null
//        );
//
//        // Then
//        assertNotNull(executor);
//    }
//
//    @Test
//    void testRun_WithNoTasks_ShouldReturnEarly() throws SQLException {
//        // Given
//        DataPhantomPlaygroundExecutor executor = new DataPhantomPlaygroundExecutor(
//            playground, taskDAO, playgroundDAO,null, emrService,
//            PlaygroundExecutionType.AD_HOC, cancelPlaygroundRequestSet, false, null
//        );
//
//
// when(taskDAO.findTasksByPlaygroundRecursively(playground.getId())).thenReturn(Collections.emptyList());
//
//        // When
//        executor.run();
//
//        // Then
//        verify(taskDAO).findTasksByPlaygroundRecursively(playground.getId());
//        verifyNoInteractions(emrService);
//    }
//
//    @Test
//    void testRun_WithRunningPlayground_ShouldSkipExecution() throws SQLException {
//        // Given
//        DataPhantomPlaygroundExecutor executor = new DataPhantomPlaygroundExecutor(
//            playground, taskDAO, playgroundDAO,null, emrService,
//            PlaygroundExecutionType.AD_HOC, cancelPlaygroundRequestSet, false, null
//        );
//
//
//        // When
//        executor.run();
//
//        // Then
//        verifyNoInteractions(taskDAO);
//        verifyNoInteractions(emrService);
//    }
//
//    @Test
//    void testRun_WithDatabaseException_ShouldThrowRuntimeException() throws SQLException {
//        // Given
//        DataPhantomPlaygroundExecutor executor = new DataPhantomPlaygroundExecutor(
//            playground, taskDAO, playgroundDAO,null, emrService,
//            PlaygroundExecutionType.AD_HOC, cancelPlaygroundRequestSet, false, null
//        );
//
//
//        // When & Then
//        assertThrows(RuntimeException.class, () -> executor.run());
//    }
//
//    @Test
//    void testRun_WithCancellationRequest_ShouldSkipExecution() throws SQLException {
//        // Given
//        DataPhantomPlaygroundExecutor executor = new DataPhantomPlaygroundExecutor(
//            playground, taskDAO, playgroundDAO,null, emrService,
//            PlaygroundExecutionType.AD_HOC, cancelPlaygroundRequestSet, false, null
//        );
//
//        // Add playground to cancel set
//        cancelPlaygroundRequestSet.add(playground.getId().toString());
//
//
// when(taskDAO.findTasksByPlaygroundRecursively(playground.getId())).thenReturn(Collections.emptyList());
//
//        // When
//        executor.run();
//
//        // Then
//        verify(taskDAO).findTasksByPlaygroundRecursively(playground.getId());
//        verifyNoInteractions(emrService);
//    }
//
//    @Test
//    void testRun_WithScheduledType_ShouldExecuteNormally() throws SQLException {
//        // Given
//        DataPhantomPlaygroundExecutor executor = new DataPhantomPlaygroundExecutor(
//            playground, taskDAO, playgroundDAO,null, emrService,
//            PlaygroundExecutionType.SCHEDULED, cancelPlaygroundRequestSet, false, null
//        );
//
//
// when(taskDAO.findTasksByPlaygroundRecursively(playground.getId())).thenReturn(Collections.emptyList());
//
//        // When
//        executor.run();
//
//        // Then
//        verify(taskDAO).findTasksByPlaygroundRecursively(playground.getId());
//        verifyNoInteractions(emrService);
//    }
//
//    // Helper methods
//    private Task createTask(String name, UUID parentId, String query) {
//        Task task = new Task();
//        task.setId(UUID.randomUUID());
//        task.setName(name);
//        task.setParentId(parentId);
//        task.setQuery(query);
//        task.setPlaygroundId(playground.getId());
//        return task;
//    }
//
//    private EMRServiceImpl.StepResult createSuccessStepResult(String queryId) {
//        return new EMRServiceImpl.StepResult(
//            "step-" + queryId,
//            StepState.COMPLETED,
//            "Success",
//            "s3://bucket/output/" + queryId + ".txt",
//            "s3://bucket/logs/" + queryId + ".log",
//            queryId
//        );
//    }
//
//    private EMRServiceImpl.StepResult createFailureStepResult(String queryId) {
//        return new EMRServiceImpl.StepResult(
//            "step-" + queryId,
//            StepState.FAILED,
//            "Failed",
//            null,
//            "s3://bucket/logs/" + queryId + ".log",
//            queryId
//        );
//    }
// }
