package com.annihilator.data.playground.cloud.aws;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class S3ServiceImplTest {

    @Mock
    private S3Client s3Client;
    
    private S3ServiceImpl s3Service;

    @BeforeEach
    void setUp() {
        s3Service = new S3ServiceImpl(s3Client, "test-bucket", "test-prefix");
    }

    @Test
    void testWriteQueryToS3_WithValidData_ShouldReturnS3Key() {
        // Given
        String queryText = "SELECT 1";
        String fileName = "test.sql";

        when(s3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
            .thenReturn(PutObjectResponse.builder().build());

        // When
        String result = s3Service.writeQueryToS3(queryText, fileName);

        // Then
        assertNotNull(result);
        assertTrue(result.contains(fileName));
        verify(s3Client).putObject(any(PutObjectRequest.class), any(RequestBody.class));
    }

    @Test
    void testWriteQueryToS3_WithException_ShouldThrowException() {
        // Given
        String queryText = "SELECT 1";
        String fileName = "test.sql";

        when(s3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
            .thenThrow(new RuntimeException("S3 error"));

        // When & Then
        assertThrows(RuntimeException.class, () -> s3Service.writeQueryToS3(queryText, fileName));
    }

    @Test
    void testReadOutputPreview_WithValidS3Path_ShouldReturnPreview() {
        // Given
        String s3Path = "s3://test-bucket/output.txt";
        int maxLines = 10;
        String content = "Line 1\nLine 2\nLine 3\nLine 4\nLine 5";
        InputStream inputStream = new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
        ResponseInputStream<GetObjectResponse> responseInputStream = 
            new ResponseInputStream<>(GetObjectResponse.builder().build(), inputStream);

        when(s3Client.getObject(any(GetObjectRequest.class)))
            .thenReturn(responseInputStream);

        // When
        List<String> result = s3Service.readOutputPreview(s3Path, maxLines);

        // Then
        assertNotNull(result);
        assertTrue(result.size() <= maxLines);
        verify(s3Client).getObject(any(GetObjectRequest.class));
    }

    @Test
    void testReadOutputPreview_WithDefaultMaxLines_ShouldReturnPreview() {
        // Given
        String s3Path = "s3://test-bucket/output.txt";
        String content = "Line 1\nLine 2\nLine 3";
        InputStream inputStream = new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
        ResponseInputStream<GetObjectResponse> responseInputStream = 
            new ResponseInputStream<>(GetObjectResponse.builder().build(), inputStream);

        when(s3Client.getObject(any(GetObjectRequest.class)))
            .thenReturn(responseInputStream);

        // When
        List<String> result = s3Service.readOutputPreview(s3Path);

        // Then
        assertNotNull(result);
        assertTrue(result.size() <= 100); // Default max lines
        verify(s3Client).getObject(any(GetObjectRequest.class));
    }

    @Test
    void testReadOutputPreview_WithInvalidS3Path_ShouldThrowException() {
        // Given
        String invalidS3Path = "invalid-path";

        // When & Then
        assertThrows(RuntimeException.class, () -> s3Service.readOutputPreview(invalidS3Path));
    }

    @Test
    void testReadOutputPreview_WithNullS3Path_ShouldThrowException() {
        // Given
        String nullS3Path = null;

        // When & Then
        assertThrows(RuntimeException.class, () -> s3Service.readOutputPreview(nullS3Path));
    }

    @Test
    void testReadOutputPreview_WithEmptyS3Path_ShouldThrowException() {
        // Given
        String emptyS3Path = "";

        // When & Then
        assertThrows(RuntimeException.class, () -> s3Service.readOutputPreview(emptyS3Path));
    }

    @Test
    void testReadOutputPreview_WithException_ShouldThrowException() {
        // Given
        String s3Path = "s3://test-bucket/output.txt";

        when(s3Client.getObject(any(GetObjectRequest.class)))
            .thenThrow(new RuntimeException("S3 error"));

        // When & Then
        assertThrows(RuntimeException.class, () -> s3Service.readOutputPreview(s3Path));
    }

    @Test
    void testClose_ShouldCloseClient() {
        // Given
        // No setup needed

        // When
        s3Service.close();

        // Then
        // Verify no exceptions are thrown
        assertDoesNotThrow(() -> s3Service.close());
    }

    @Test
    void testClose_WithException_ShouldHandleGracefully() {
        // Given
        // No setup needed

        // When & Then
        // Should not throw exception even if close fails
        assertDoesNotThrow(() -> s3Service.close());
    }

    @Test
    void testConstructor_WithValidParameters_ShouldCreateInstance() {
        // Given
        String bucketName = "test-bucket";
        S3Client client = mock(S3Client.class);

        // When
        S3ServiceImpl service = new S3ServiceImpl(client, bucketName, "test-prefix");

        // Then
        assertNotNull(service);
    }

    @Test
    void testConstructor_WithNullParameters_ShouldCreateInstance() {
        // Given
        String bucketName = null;
        S3Client client = null;

        // When
        S3ServiceImpl service = new S3ServiceImpl(client, bucketName, "test-prefix");

        // Then
        assertNotNull(service);
    }

    @Test
    void testFindFirstDataFileInDirectory_WithValidDirectory_ShouldReturnFirstDataFile() {
        // Given
        String s3DirectoryPath = "s3://test-bucket/sparksql-output/2025-09-14/playground1/query1/unique1/";
        
        S3Object successFile = S3Object.builder()
            .key("sparksql-output/2025-09-14/playground1/query1/unique1/_SUCCESS")
            .build();
        
        S3Object dataFile1 = S3Object.builder()
            .key("sparksql-output/2025-09-14/playground1/query1/unique1/part-00000-abc123.csv")
            .build();
        
        S3Object dataFile2 = S3Object.builder()
            .key("sparksql-output/2025-09-14/playground1/query1/unique1/part-00001-def456.csv")
            .build();
        
        ListObjectsV2Response listResponse = ListObjectsV2Response.builder()
            .contents(java.util.Arrays.asList(successFile, dataFile1, dataFile2))
            .build();
        
        when(s3Client.listObjectsV2(any(ListObjectsV2Request.class)))
            .thenReturn(listResponse);

        // When
        String result = s3Service.findFirstDataFileInDirectory(s3DirectoryPath);

        // Then
        assertNotNull(result);
        assertEquals("s3://test-bucket/sparksql-output/2025-09-14/playground1/query1/unique1/part-00000-abc123.csv", result);
        verify(s3Client).listObjectsV2(any(ListObjectsV2Request.class));
    }

    @Test
    void testFindFirstFileInDirectory_WithEmptyDirectory_ShouldReturnNull() {
        // Given
        String s3DirectoryPath = "s3://test-bucket/empty-directory/";
        
        ListObjectsV2Response listResponse = ListObjectsV2Response.builder()
            .contents(java.util.Collections.emptyList()) // Empty list
            .build();
        
        when(s3Client.listObjectsV2(any(ListObjectsV2Request.class)))
            .thenReturn(listResponse);

        // When
        String result = s3Service.findFirstDataFileInDirectory(s3DirectoryPath);

        // Then
        assertNull(result);
        verify(s3Client).listObjectsV2(any(ListObjectsV2Request.class));
    }

    @Test
    void testFindFirstFileInDirectory_WithException_ShouldReturnNull() {
        // Given
        String s3DirectoryPath = "s3://test-bucket/directory/";
        
        when(s3Client.listObjectsV2(any(ListObjectsV2Request.class)))
            .thenThrow(new RuntimeException("S3 error"));

        // When
        String result = s3Service.findFirstDataFileInDirectory(s3DirectoryPath);

        // Then
        assertNull(result);
        verify(s3Client).listObjectsV2(any(ListObjectsV2Request.class));
    }

    @Test
    void testFindFirstFileInDirectory_WithOnlyMetadataFiles_ShouldReturnNull() {
        // Given
        String s3DirectoryPath = "s3://test-bucket/sparksql-output/2025-09-14/playground1/query1/unique1/";
        
        S3Object successFile = S3Object.builder()
            .key("sparksql-output/2025-09-14/playground1/query1/unique1/_SUCCESS")
            .build();
        
        S3Object crcFile = S3Object.builder()
            .key("sparksql-output/2025-09-14/playground1/query1/unique1/part-00000-abc123.csv.crc")
            .build();
        
        S3Object metaFile = S3Object.builder()
            .key("sparksql-output/2025-09-14/playground1/query1/unique1/_temporary")
            .build();
        
        ListObjectsV2Response listResponse = ListObjectsV2Response.builder()
            .contents(java.util.Arrays.asList(successFile, crcFile, metaFile))
            .build();
        
        when(s3Client.listObjectsV2(any(ListObjectsV2Request.class)))
            .thenReturn(listResponse);

        // When
        String result = s3Service.findFirstDataFileInDirectory(s3DirectoryPath);

        // Then
        assertNull(result);
        verify(s3Client).listObjectsV2(any(ListObjectsV2Request.class));
    }

    @Test
    void testFindFirstFileInDirectory_WithMixedFiles_ShouldReturnDataFile() {
        // Given
        String s3DirectoryPath = "s3://test-bucket/sparksql-output/2025-09-14/playground1/query1/unique1/";
        
        S3Object successFile = S3Object.builder()
            .key("sparksql-output/2025-09-14/playground1/query1/unique1/_SUCCESS")
            .build();
        
        S3Object crcFile = S3Object.builder()
            .key("sparksql-output/2025-09-14/playground1/query1/unique1/part-00000-abc123.csv.crc")
            .build();
        
        S3Object dataFile = S3Object.builder()
            .key("sparksql-output/2025-09-14/playground1/query1/unique1/part-00000-abc123.csv")
            .build();
        
        ListObjectsV2Response listResponse = ListObjectsV2Response.builder()
            .contents(java.util.Arrays.asList(successFile, crcFile, dataFile))
            .build();
        
        when(s3Client.listObjectsV2(any(ListObjectsV2Request.class)))
            .thenReturn(listResponse);

        // When
        String result = s3Service.findFirstDataFileInDirectory(s3DirectoryPath);

        // Then
        assertNotNull(result);
        assertEquals("s3://test-bucket/sparksql-output/2025-09-14/playground1/query1/unique1/part-00000-abc123.csv", result);
        verify(s3Client).listObjectsV2(any(ListObjectsV2Request.class));
    }

    @Test
    void testReadOutputPreview_WithDirectoryPath_ShouldFindDataFileAndRead() {
        // Given
        String s3DirectoryPath = "s3://test-bucket/sparksql-output/2025-09-14/playground1/query1/unique1/";
        String expectedDataFile = "s3://test-bucket/sparksql-output/2025-09-14/playground1/query1/unique1/part-00000-abc123.csv";
        int maxLines = 10;
        String content = "Line 1\nLine 2\nLine 3\nLine 4\nLine 5";
        InputStream inputStream = new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
        ResponseInputStream<GetObjectResponse> responseInputStream = 
            new ResponseInputStream<>(GetObjectResponse.builder().build(), inputStream);
        
        // Mock the directory listing
        S3Object dataFile = S3Object.builder()
            .key("sparksql-output/2025-09-14/playground1/query1/unique1/part-00000-abc123.csv")
            .build();
        
        ListObjectsV2Response listResponse = ListObjectsV2Response.builder()
            .contents(java.util.Arrays.asList(dataFile))
            .build();
        
        when(s3Client.listObjectsV2(any(ListObjectsV2Request.class)))
            .thenReturn(listResponse);
        when(s3Client.getObject(any(GetObjectRequest.class)))
            .thenReturn(responseInputStream);

        // When
        List<String> result = s3Service.readOutputPreview(s3DirectoryPath, maxLines);

        // Then
        assertNotNull(result);
        assertTrue(result.size() <= maxLines);
        verify(s3Client).listObjectsV2(any(ListObjectsV2Request.class));
        verify(s3Client).getObject(any(GetObjectRequest.class));
    }

    @Test
    void testReadOutputPreview_WithFilePath_ShouldReadDirectly() {
        // Given
        String s3FilePath = "s3://test-bucket/output.txt";
        int maxLines = 10;
        String content = "Line 1\nLine 2\nLine 3\nLine 4\nLine 5";
        InputStream inputStream = new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
        ResponseInputStream<GetObjectResponse> responseInputStream = 
            new ResponseInputStream<>(GetObjectResponse.builder().build(), inputStream);

        when(s3Client.getObject(any(GetObjectRequest.class)))
            .thenReturn(responseInputStream);

        // When
        List<String> result = s3Service.readOutputPreview(s3FilePath, maxLines);

        // Then
        assertNotNull(result);
        assertTrue(result.size() <= maxLines);
        verify(s3Client).getObject(any(GetObjectRequest.class));
        // Should not call listObjectsV2 for file paths
        verify(s3Client, never()).listObjectsV2(any(ListObjectsV2Request.class));
    }

    @Test
    void testReadOutputPreview_WithEmptyDirectory_ShouldFallbackToDirectoryPath() {
        // Given
        String s3DirectoryPath = "s3://test-bucket/empty-directory/";
        int maxLines = 10;
        String content = "Line 1\nLine 2\nLine 3";
        InputStream inputStream = new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
        ResponseInputStream<GetObjectResponse> responseInputStream = 
            new ResponseInputStream<>(GetObjectResponse.builder().build(), inputStream);
        
        // Mock empty directory listing
        ListObjectsV2Response listResponse = ListObjectsV2Response.builder()
            .contents(java.util.Collections.emptyList())
            .build();
        
        when(s3Client.listObjectsV2(any(ListObjectsV2Request.class)))
            .thenReturn(listResponse);
        when(s3Client.getObject(any(GetObjectRequest.class)))
            .thenReturn(responseInputStream);

        // When
        List<String> result = s3Service.readOutputPreview(s3DirectoryPath, maxLines);

        // Then
        assertNotNull(result);
        assertTrue(result.size() <= maxLines);
        verify(s3Client).listObjectsV2(any(ListObjectsV2Request.class));
        verify(s3Client).getObject(any(GetObjectRequest.class));
    }
}