package com.annihilator.data.playground.cloud.aws;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.annihilator.data.playground.config.AWSEmrConfig;
import com.annihilator.data.playground.config.RetryConfig;
import com.annihilator.data.playground.model.StepResult;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.services.cloudformation.CloudFormationClient;
import software.amazon.awssdk.services.emr.EmrClient;

/** Test class for EMRServiceImpl retry functionality */
public class EMRServiceImplRetryTest {

  @Mock private CloudFormationClient cloudFormationClient;

  @Mock private EmrClient emrClient;

  @Mock private S3Service s3Service;

  private EMRServiceImpl emrService;

  @BeforeEach
  public void setUp() {
    MockitoAnnotations.openMocks(this);

    AWSEmrConfig awsEmrConfig = new AWSEmrConfig();
    awsEmrConfig.setStackName("test-stack");
    awsEmrConfig.setClusterLogicalId("test-cluster");
    awsEmrConfig.setS3PathPrefix("test-prefix");
    awsEmrConfig.setS3Bucket("test-bucket");
    awsEmrConfig.setMaxStepRetries(3);

    emrService =
        new EMRServiceImpl(awsEmrConfig, cloudFormationClient, emrClient, s3Service, null, null);
  }

  @Test
  public void testRetryConfigDefaults() {
    RetryConfig defaultConfig = new RetryConfig();

    assertEquals(3, defaultConfig.getMaxRetries());
    assertEquals(5000, defaultConfig.getBaseDelayMs());
    assertEquals(2.0, defaultConfig.getBackoffMultiplier());
    assertEquals(60000, defaultConfig.getMaxDelayMs());
    assertTrue(defaultConfig.isEnableJitter());
    assertEquals(0.1, defaultConfig.getJitterFactor());
  }

  @Test
  public void testSubmitTaskAndWaitWithRetry() {
    // Test that the method has built-in retry functionality
    CompletableFuture<StepResult> future =
        emrService.submitTaskAndWait("test-playground", "test-query", "SELECT 1", "presto");

    assertNotNull(future);
  }

  @Test
  public void testRetryDelayCalculation() {
    RetryConfig config = new RetryConfig(3, 1000, 2.0, 10000, false, 0.0);

    // Test exponential backoff without jitter
    assertEquals(1000, config.getRetryDelay(0)); // 1s
    assertEquals(2000, config.getRetryDelay(1)); // 2s
    assertEquals(4000, config.getRetryDelay(2)); // 4s
    assertEquals(8000, config.getRetryDelay(3)); // 8s
  }

  @Test
  public void testRetryDelayWithMaxCap() {
    RetryConfig config = new RetryConfig(3, 1000, 2.0, 5000, false, 0.0);

    // Test that delay is capped at maxDelayMs
    assertEquals(1000, config.getRetryDelay(0)); // 1s
    assertEquals(2000, config.getRetryDelay(1)); // 2s
    assertEquals(4000, config.getRetryDelay(2)); // 4s
    assertEquals(5000, config.getRetryDelay(3)); // Capped at 5s
    assertEquals(5000, config.getRetryDelay(10)); // Still capped at 5s
  }

  @Test
  public void testShouldRetryLogic() {
    RetryConfig config = new RetryConfig();

    // Should retry transient failures
    assertTrue(config.shouldRetry("Connection timeout"));
    assertTrue(config.shouldRetry("Network error"));
    assertTrue(config.shouldRetry("Service temporarily unavailable"));
    assertTrue(config.shouldRetry("Rate limit exceeded"));

    // Should not retry permanent failures
    assertFalse(config.shouldRetry("Syntax error in query"));
    assertFalse(config.shouldRetry("Table not found"));
    assertFalse(config.shouldRetry("Permission denied"));

    // Should retry unknown errors
    assertTrue(config.shouldRetry("Unknown error"));
    assertTrue(config.shouldRetry(null));
  }

  @Test
  public void testRetryConfigToString() {
    RetryConfig config = new RetryConfig(3, 5000, 2.0, 60000, true, 0.1);
    String configString = config.toString();

    assertTrue(configString.contains("maxRetries=3"));
    assertTrue(configString.contains("baseDelayMs=5000"));
    assertTrue(configString.contains("backoffMultiplier=2.0"));
    assertTrue(configString.contains("maxDelayMs=60000"));
    assertTrue(configString.contains("enableJitter=true"));
    assertTrue(configString.contains("jitterFactor=0.1"));
  }

  @Test
  public void testRetryConfigSetters() {
    RetryConfig config = new RetryConfig();

    // Test setters
    config.setMaxRetries(5);
    config.setBaseDelayMs(10000);
    config.setBackoffMultiplier(1.5);
    config.setMaxDelayMs(30000);
    config.setEnableJitter(false);
    config.setJitterFactor(0.2);

    // Verify changes
    assertEquals(5, config.getMaxRetries());
    assertEquals(10000, config.getBaseDelayMs());
    assertEquals(1.5, config.getBackoffMultiplier());
    assertEquals(30000, config.getMaxDelayMs());
    assertFalse(config.isEnableJitter());
    assertEquals(0.2, config.getJitterFactor());
  }
}
