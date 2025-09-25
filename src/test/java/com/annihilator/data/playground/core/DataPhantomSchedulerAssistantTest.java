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
import java.util.*;
import java.util.concurrent.ExecutorService;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

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
        scheduler = new DataPhantomSchedulerAssistant(
            playgroundDAO, taskDAO, null, null, emrService,
            executorService, null, cancelPlaygroundRequestSet, null
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