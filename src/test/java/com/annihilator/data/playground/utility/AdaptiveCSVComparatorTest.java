// package com.annihilator.data.playground.utility;
//
// import com.annihilator.data.playground.cloud.aws.S3Service;
// import com.annihilator.data.playground.db.ReconciliationMappingDAO;
// import com.annihilator.data.playground.db.ReconciliationResultsDAO;
// import com.annihilator.data.playground.db.TaskDAO;
// import com.annihilator.data.playground.model.Reconciliation;
// import com.annihilator.data.playground.model.Status;
// import com.annihilator.data.playground.model.Task;
// import org.junit.jupiter.api.BeforeEach;
// import org.junit.jupiter.api.Test;
// import org.junit.jupiter.api.extension.ExtendWith;
// import org.mockito.Mock;
// import org.mockito.junit.jupiter.MockitoExtension;
//
// import java.sql.SQLException;
// import java.util.Arrays;
// import java.util.Collections;
// import java.util.List;
// import java.util.UUID;
//
// import static org.junit.jupiter.api.Assertions.*;
// import static org.mockito.ArgumentMatchers.*;
// import static org.mockito.Mockito.*;
//
// @ExtendWith(MockitoExtension.class)
// class AdaptiveCSVComparatorTest {
//
//    @Mock
//    private S3Service s3Service;
//
//    @Mock
//    private ReconciliationMappingDAO reconciliationMappingDAO;
//
//    @Mock
//    private ReconciliationResultsDAO reconciliationResultsDAO;
//
//    @Mock
//    private TaskDAO taskDAO;
//
//    private AdaptiveCSVComparator adaptiveCSVComparator;
//
//    @BeforeEach
//    void setUp() {
//        adaptiveCSVComparator = new AdaptiveCSVComparator(
//            s3Service, reconciliationMappingDAO, reconciliationResultsDAO, taskDAO);
//    }
//
//    @Test
//    void testCompare_NoReconciliationFound() throws SQLException {
//        // Given
//        String reconciliationId = "test-reconciliation-id";
//        when(reconciliationMappingDAO.findReconciliationMappingByPlaygroundId(reconciliationId))
//            .thenReturn(Collections.emptyList());
//
//        // When
//        adaptiveCSVComparator.compare(reconciliationId);
//
//        // Then
//
// verify(reconciliationMappingDAO).findReconciliationMappingByPlaygroundId(reconciliationId);
//        verifyNoInteractions(taskDAO);
//        verifyNoInteractions(s3Service);
//    }
//
//    @Test
//    void testCompare_TasksNotReady() throws SQLException {
//        // Given
//        String reconciliationId = "test-reconciliation-id";
//        UUID reconciliationUuid = UUID.randomUUID();
//
//        Reconciliation reconciliation = new Reconciliation();
//        reconciliation.setReconciliationId(reconciliationUuid);
//        reconciliation.setLeftTableId(1L);
//        reconciliation.setRightTableId(2L);
//        reconciliation.setMapping("{\"col1\":\"col2\"}");
//
//        when(reconciliationMappingDAO.findReconciliationMappingByPlaygroundId(reconciliationId))
//            .thenReturn(Arrays.asList(reconciliation));
//
//        // Mock tasks that are not ready (null status)
//        Task leftTask = new Task();
//        leftTask.setId(1L);
//        leftTask.setOutputLocation("s3://bucket/left.csv");
//        leftTask.setLastRunStatus(null); // Not ready
//
//        Task rightTask = new Task();
//        rightTask.setId(2L);
//        rightTask.setOutputLocation("s3://bucket/right.csv");
//        rightTask.setLastRunStatus(Status.SUCCESS);
//
//        when(taskDAO.findTaskById(1L)).thenReturn(leftTask);
//        when(taskDAO.findTaskById(2L)).thenReturn(rightTask);
//
//        // When
//        adaptiveCSVComparator.compare(reconciliationId);
//
//        // Then
//        verify(reconciliationResultsDAO).upsertReconciliationResult(
//            eq(reconciliationId), any(), eq("FAILED"));
//    }
//
//    @Test
//    void testCompare_FileSizeBasedStrategySelection() throws SQLException {
//        // Given
//        String reconciliationId = "test-reconciliation-id";
//        UUID reconciliationUuid = UUID.randomUUID();
//
//        Reconciliation reconciliation = new Reconciliation();
//        reconciliation.setReconciliationId(reconciliationUuid);
//        reconciliation.setLeftTableId(1L);
//        reconciliation.setRightTableId(2L);
//        reconciliation.setMapping("{\"col1\":\"col2\"}");
//
//        when(reconciliationMappingDAO.findReconciliationMappingByPlaygroundId(reconciliationId))
//            .thenReturn(Arrays.asList(reconciliation));
//
//        // Mock ready tasks
//        Task leftTask = new Task();
//        leftTask.setId(1L);
//        leftTask.setOutputLocation("s3://bucket/left.csv");
//        leftTask.setLastRunStatus(Status.SUCCESS);
//
//        Task rightTask = new Task();
//        rightTask.setId(2L);
//        rightTask.setOutputLocation("s3://bucket/right.csv");
//        rightTask.setLastRunStatus(Status.SUCCESS);
//
//        when(taskDAO.findTaskById(1L)).thenReturn(leftTask);
//        when(taskDAO.findTaskById(2L)).thenReturn(rightTask);
//
//        // Mock file sizes - small files (should use exact matching)
//        when(s3Service.getS3FileSize("s3://bucket/left.csv")).thenReturn(1024L); // 1KB
//        when(s3Service.getS3FileSize("s3://bucket/right.csv")).thenReturn(2048L); // 2KB
//        when(s3Service.getBucketName()).thenReturn("test-bucket");
//
//        // When
//        adaptiveCSVComparator.compare(reconciliationId);
//
//        // Then
//        verify(s3Service).getS3FileSize("s3://bucket/left.csv");
//        verify(s3Service).getS3FileSize("s3://bucket/right.csv");
//        // Should use exact matching for small files
//        verify(s3Service, atLeastOnce()).readFileLineByLine(anyString(), any());
//    }
//
//    @Test
//    void testCompare_LargeFilesUseBloomFilter() throws SQLException {
//        // Given
//        String reconciliationId = "test-reconciliation-id";
//        UUID reconciliationUuid = UUID.randomUUID();
//
//        Reconciliation reconciliation = new Reconciliation();
//        reconciliation.setReconciliationId(reconciliationUuid);
//        reconciliation.setLeftTableId(1L);
//        reconciliation.setRightTableId(2L);
//        reconciliation.setMapping("{\"col1\":\"col2\"}");
//
//        when(reconciliationMappingDAO.findReconciliationMappingByPlaygroundId(reconciliationId))
//            .thenReturn(Arrays.asList(reconciliation));
//
//        // Mock ready tasks
//        Task leftTask = new Task();
//        leftTask.setId(1L);
//        leftTask.setOutputLocation("s3://bucket/left.csv");
//        leftTask.setLastRunStatus(Status.SUCCESS);
//
//        Task rightTask = new Task();
//        rightTask.setId(2L);
//        rightTask.setOutputLocation("s3://bucket/right.csv");
//        rightTask.setLastRunStatus(Status.SUCCESS);
//
//        when(taskDAO.findTaskById(1L)).thenReturn(leftTask);
//        when(taskDAO.findTaskById(2L)).thenReturn(rightTask);
//
//        // Mock file sizes - large files (should use Bloom filter)
//        long largeFileSize = 30 * 1024 * 1024L; // 30MB each
//        when(s3Service.getS3FileSize("s3://bucket/left.csv")).thenReturn(largeFileSize);
//        when(s3Service.getS3FileSize("s3://bucket/right.csv")).thenReturn(largeFileSize);
//
//        // When
//        adaptiveCSVComparator.compare(reconciliationId);
//
//        // Then
//        verify(s3Service).getS3FileSize("s3://bucket/left.csv");
//        verify(s3Service).getS3FileSize("s3://bucket/right.csv");
//        // Should use Bloom filter for large files
//        verify(s3Service, atLeastOnce()).readFileLineByLine(anyString(), any());
//    }
//
//    @Test
//    void testCompare_FileSizeCheckFails() throws SQLException {
//        // Given
//        String reconciliationId = "test-reconciliation-id";
//        UUID reconciliationUuid = UUID.randomUUID();
//
//        Reconciliation reconciliation = new Reconciliation();
//        reconciliation.setReconciliationId(reconciliationUuid);
//        reconciliation.setLeftTableId(1L);
//        reconciliation.setRightTableId(2L);
//        reconciliation.setMapping("{\"col1\":\"col2\"}");
//
//        when(reconciliationMappingDAO.findReconciliationMappingByPlaygroundId(reconciliationId))
//            .thenReturn(Arrays.asList(reconciliation));
//
//        // Mock ready tasks
//        Task leftTask = new Task();
//        leftTask.setId(1L);
//        leftTask.setOutputLocation("s3://bucket/left.csv");
//        leftTask.setLastRunStatus(Status.SUCCESS);
//
//        Task rightTask = new Task();
//        rightTask.setId(2L);
//        rightTask.setOutputLocation("s3://bucket/right.csv");
//        rightTask.setLastRunStatus(Status.SUCCESS);
//
//        when(taskDAO.findTaskById(1L)).thenReturn(leftTask);
//        when(taskDAO.findTaskById(2L)).thenReturn(rightTask);
//
//        // Mock file size check failure
//        when(s3Service.getS3FileSize("s3://bucket/left.csv")).thenReturn(-1L);
//        when(s3Service.getS3FileSize("s3://bucket/right.csv")).thenReturn(1024L);
//
//        // When
//        adaptiveCSVComparator.compare(reconciliationId);
//
//        // Then
//        verify(reconciliationResultsDAO).upsertReconciliationResult(
//            eq(reconciliationId), any(), eq("FAILED"));
//    }
// }
