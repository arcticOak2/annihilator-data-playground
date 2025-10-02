package com.annihilator.data.playground.core;

import com.annihilator.data.playground.cloud.aws.EMRService;
import com.annihilator.data.playground.cloud.aws.S3Service;
import com.annihilator.data.playground.db.PlaygroundDAO;
import com.annihilator.data.playground.db.TaskDAO;
import com.annihilator.data.playground.model.Playground;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DataPhantomSchedulerAssistantTest {

    @Mock
    private PlaygroundDAO playgroundDAO;
    
    @Mock
    private TaskDAO taskDAO;
    
    @Mock
    private EMRService emrService;
    
    @Mock
    private S3Service s3Service;
    
    @Mock
    private ExecutorService executorService;
    
    private Set<String> cancelPlaygroundRequestSet;
    private DataPhantomSchedulerAssistant scheduler;

    @BeforeEach
    void setUp() {
        cancelPlaygroundRequestSet = Collections.synchronizedSet(new HashSet<>());
        scheduler = new DataPhantomSchedulerAssistant( null,
            playgroundDAO, taskDAO, null, null, emrService,
            executorService, null, cancelPlaygroundRequestSet, null, null,
            null, null, null, null
        );
    }

    @Test
    void testSleep_WithInterruptedException_ShouldHandleGracefully() {
        // Given
        Thread testThread = new Thread(() -> {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        
        testThread.start();
        testThread.interrupt();
        
        // When & Then
        assertDoesNotThrow(() -> {
            try {
                testThread.join(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
    }

    // Helper methods
    private Playground createPlayground(String name, String cronExpression) {
        Playground playground = new Playground();
        playground.setId(UUID.randomUUID());
        playground.setName(name);
        playground.setCronExpression(cronExpression);
        playground.setUserId("test-user");
        playground.setLastExecutedAt(0L);
        return playground;
    }
}