package com.annihilator.data.playground.cloud.aws;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.annihilator.data.playground.config.AWSEmrConfig;
import java.lang.reflect.Method;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.cloudformation.CloudFormationClient;
import software.amazon.awssdk.services.emr.EmrClient;

@ExtendWith(MockitoExtension.class)
class EMRServiceImplTest {

  @Mock private EmrClient emrClient;

  @Mock private CloudFormationClient cloudFormationClient;

  private EMRServiceImpl emrService;

  @BeforeEach
  void setUp() {
    AWSEmrConfig awsEmrConfig = new AWSEmrConfig();
    awsEmrConfig.setStackName("test-stack");
    awsEmrConfig.setClusterLogicalId("test-cluster");
    awsEmrConfig.setS3PathPrefix("test-prefix");
    awsEmrConfig.setMaxStepRetries(3);

    emrService =
        new EMRServiceImpl(awsEmrConfig, cloudFormationClient, emrClient, null, null, null);
  }

  @Test
  void testConstructor_WithValidParameters_ShouldCreateInstance() {
    // Given

    AWSEmrConfig awsEmrConfig = new AWSEmrConfig();
    awsEmrConfig.setStackName("test-stack");
    awsEmrConfig.setClusterLogicalId("test-cluster");
    awsEmrConfig.setS3PathPrefix("test-prefix");
    awsEmrConfig.setS3Bucket("test-bucket");
    awsEmrConfig.setMaxStepRetries(3);

    // When
    EMRServiceImpl service =
        new EMRServiceImpl(awsEmrConfig, cloudFormationClient, emrClient, null, null, null);

    // Then
    assertNotNull(service);
  }

  @Test
  void testClose_ShouldCloseEmrClient() {
    // When
    emrService.close();

    // Then
    verify(emrClient).close();
  }

  @Test
  void testClose_WithException_ShouldHandleGracefully() {
    // Given
    doThrow(new RuntimeException("Close error")).when(emrClient).close();

    // When & Then
    assertDoesNotThrow(() -> emrService.close());
    verify(emrClient).close();
  }

  @Test
  void testClose_ShouldCloseCloudFormationClient() {
    // When
    emrService.close();

    // Then
    verify(cloudFormationClient).close();
  }

  @Test
  void testClose_WithCloudFormationException_ShouldHandleGracefully() {
    // Given
    doThrow(new RuntimeException("CloudFormation close error")).when(cloudFormationClient).close();

    // When & Then
    assertDoesNotThrow(() -> emrService.close());
    verify(cloudFormationClient).close();
  }

  @Test
  void testGenerateSparkSQLScriptFromTemplate_WithSemicolon_ShouldRemoveSemicolon()
      throws Exception {
    // Given
    String queryWithSemicolon = "SELECT * FROM test_table;";
    String playgroundId = "test-playground";
    String queryId = "test-query";
    String uniqueId = "test-unique";
    String currentDate = "2024-01-01";
    String outputBucket = "test-bucket";

    // Use reflection to access the private method
    Method method =
        EMRServiceImpl.class.getDeclaredMethod(
            "generateSparkSQLScriptFromTemplate",
            String.class,
            String.class,
            String.class,
            String.class,
            String.class,
            String.class);
    method.setAccessible(true);

    // When
    String result =
        (String)
            method.invoke(
                emrService,
                queryWithSemicolon,
                playgroundId,
                queryId,
                uniqueId,
                currentDate,
                outputBucket);

    // Then
    assertNotNull(result);
    // The generated script should not contain the semicolon in the spark.sql() call
    assertFalse(result.contains("spark.sql(\"SELECT * FROM test_table;\")"));
    assertTrue(result.contains("spark.sql(\"SELECT * FROM test_table\")"));
  }

  @Test
  void testGenerateSparkSQLScriptFromTemplate_WithoutSemicolon_ShouldNotModifyQuery()
      throws Exception {
    // Given
    String queryWithoutSemicolon = "SELECT * FROM test_table";
    String playgroundId = "test-playground";
    String queryId = "test-query";
    String uniqueId = "test-unique";
    String currentDate = "2024-01-01";
    String outputBucket = "test-bucket";

    // Use reflection to access the private method
    Method method =
        EMRServiceImpl.class.getDeclaredMethod(
            "generateSparkSQLScriptFromTemplate",
            String.class,
            String.class,
            String.class,
            String.class,
            String.class,
            String.class);
    method.setAccessible(true);

    // When
    String result =
        (String)
            method.invoke(
                emrService,
                queryWithoutSemicolon,
                playgroundId,
                queryId,
                uniqueId,
                currentDate,
                outputBucket);

    // Then
    assertNotNull(result);
    // The generated script should contain the original query without modification
    assertTrue(result.contains("spark.sql(\"SELECT * FROM test_table\")"));
  }

  @Test
  void
      testGenerateSparkSQLScriptFromTemplate_WithMultipleSemicolons_ShouldRemoveOnlyTrailingSemicolon()
          throws Exception {
    // Given
    String queryWithMultipleSemicolons = "SELECT * FROM test_table; -- comment with semicolon;";
    String playgroundId = "test-playground";
    String queryId = "test-query";
    String uniqueId = "test-unique";
    String currentDate = "2024-01-01";
    String outputBucket = "test-bucket";

    // Use reflection to access the private method
    Method method =
        EMRServiceImpl.class.getDeclaredMethod(
            "generateSparkSQLScriptFromTemplate",
            String.class,
            String.class,
            String.class,
            String.class,
            String.class,
            String.class);
    method.setAccessible(true);

    // When
    String result =
        (String)
            method.invoke(
                emrService,
                queryWithMultipleSemicolons,
                playgroundId,
                queryId,
                uniqueId,
                currentDate,
                outputBucket);

    // Then
    assertNotNull(result);
    // The generated script should remove only the trailing semicolon
    assertTrue(
        result.contains("spark.sql(\"SELECT * FROM test_table; -- comment with semicolon\")"));
    assertFalse(
        result.contains("spark.sql(\"SELECT * FROM test_table; -- comment with semicolon;\")"));
  }
}
