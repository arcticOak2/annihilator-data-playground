package com.annihilator.data.playground.cloud.aws;

import com.annihilator.data.playground.config.AWSEmrConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

public class S3ServiceImpl implements S3Service {
    
    private static final Logger logger = LoggerFactory.getLogger(S3ServiceImpl.class);
    
    private final S3Client s3Client;
    private final AWSEmrConfig awsEmrConfig;
    
    public S3ServiceImpl(S3Client s3Client, AWSEmrConfig awsEmrConfig) {
        this.s3Client = s3Client;
        this.awsEmrConfig = awsEmrConfig;
    }
    
    @Override
    public String writeQueryToS3(String queryText, String fileName) {
        try {
            String currentDate = java.time.LocalDate.now().toString();
            
            String s3ObjectKey = String.format("%s/query/%s/%s", awsEmrConfig.getS3PathPrefix(), currentDate, fileName);
            
            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(awsEmrConfig.getS3Bucket())
                .key(s3ObjectKey)
                .contentType("text/plain")
                .build();
            
            byte[] contentBytes = queryText.getBytes(StandardCharsets.UTF_8);
            s3Client.putObject(putObjectRequest, 
                RequestBody.fromInputStream(
                    new ByteArrayInputStream(contentBytes),
                    contentBytes.length
                )
            );
            
            logger.info("Successfully uploaded query script to S3: s3://{}/{}", awsEmrConfig.getS3Bucket(), s3ObjectKey);
            return s3ObjectKey;
            
        } catch (S3Exception e) {
            logger.error("Failed to upload query script to S3: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to upload query script to S3", e);
        } catch (Exception e) {
            logger.error("Unexpected error while uploading to S3: {}", e.getMessage(), e);
            throw new RuntimeException("Unexpected error while uploading to S3", e);
        }
    }
    
    @Override
    public List<String> readOutputPreview(String s3Path) {
        try {
            String actualFilePath = determineActualFilePath(s3Path);
            
            S3PathInfo pathInfo = parseS3Path(actualFilePath);
            
            GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(pathInfo.bucketName)
                .key(pathInfo.objectKey)
                .build();
            
            List<String> previewLines = new ArrayList<>();
            
            try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(s3Client.getObject(getObjectRequest), StandardCharsets.UTF_8))) {
                
                String line;
                int lineCount = 0;
                while ((line = reader.readLine()) != null && lineCount < awsEmrConfig.getS3OutputPreviewLineCount()) {
                    previewLines.add(line);
                    lineCount++;
                }
            }
            
            logger.info("Successfully read {} lines from S3 object: s3://{}/{}", 
                previewLines.size(), pathInfo.bucketName, pathInfo.objectKey);
            
            return previewLines;
            
        } catch (Exception e) {
            logger.warn("Failed to read preview from S3 path '{}': {}", s3Path, e.getMessage());
            return Collections.emptyList();
        }
    }
    
    @Override
    public String writeReconciliationOutput(String data, String fileName) {
        try {
            String currentDate = java.time.LocalDate.now().toString();
            
            String s3ObjectKey = String.format("%s/reconciliation/%s/%s", awsEmrConfig.getS3PathPrefix(), currentDate, fileName);
            
            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(awsEmrConfig.getS3Bucket())
                .key(s3ObjectKey)
                .contentType("text/plain")
                .build();
            
            byte[] contentBytes = data.getBytes(StandardCharsets.UTF_8);
            s3Client.putObject(putObjectRequest, 
                RequestBody.fromInputStream(
                    new ByteArrayInputStream(contentBytes),
                    contentBytes.length
                )
            );
            
            String fullS3Path = String.format("s3://%s/%s", awsEmrConfig.getS3Bucket(), s3ObjectKey);
            logger.info("Successfully uploaded reconciliation output to S3: {}", fullS3Path);
            return s3ObjectKey;
            
        } catch (S3Exception e) {
            logger.error("Failed to upload reconciliation output to S3: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to upload reconciliation output to S3", e);
        } catch (Exception e) {
            logger.error("Unexpected error while uploading reconciliation output to S3: {}", e.getMessage(), e);
            throw new RuntimeException("Unexpected error while uploading reconciliation output to S3", e);
        }
    }
    
    @Override
    public void readFileLineByLine(String s3Path, Consumer<String> lineProcessor) {
        if (lineProcessor == null) {
            throw new IllegalArgumentException("Line processor cannot be null");
        }
        
        try {
            String actualFilePath = determineActualFilePath(s3Path);
            
            S3PathInfo pathInfo = parseS3Path(actualFilePath);
            
            GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(pathInfo.bucketName)
                .key(pathInfo.objectKey)
                .build();
            
            long lineCount = 0;
            try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(s3Client.getObject(getObjectRequest), StandardCharsets.UTF_8))) {
                
                String line;
                while ((line = reader.readLine()) != null) {
                    lineProcessor.accept(line);
                    lineCount++;
                }
            }
            
            logger.info("Successfully processed {} lines from S3 object: s3://{}/{}", 
                lineCount, pathInfo.bucketName, pathInfo.objectKey);
            
        } catch (S3Exception e) {
            logger.error("Failed to read from S3: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to read from S3", e);
        } catch (IOException e) {
            logger.error("IO error while reading from S3: {}", e.getMessage(), e);
            throw new RuntimeException("IO error while reading from S3", e);
        } catch (Exception e) {
            logger.error("Unexpected error while reading from S3: {}", e.getMessage(), e);
            throw new RuntimeException("Unexpected error while reading from S3", e);
        }
    }
    
    /**
     * Determines the actual file path to read from.
     * If the path is a directory (ends with /), it will find the first data file.
     * If the path is already a file, it returns the path as-is.
     */
    private String determineActualFilePath(String s3Path) {
        if (s3Path == null || s3Path.trim().isEmpty()) {
            throw new IllegalArgumentException("S3 path cannot be null or empty");
        }
        
        String trimmedPath = s3Path.trim();
        
        if (trimmedPath.endsWith("/")) {
            logger.debug("Path appears to be a directory, looking for data file: {}", trimmedPath);
            String dataFilePath = findFirstDataFileInDirectory(trimmedPath);
            if (dataFilePath != null) {
                logger.info("Found data file in directory {}: {}", trimmedPath, dataFilePath);
                return dataFilePath;
            } else {
                logger.warn("No data file found in directory: {}", trimmedPath);
                return trimmedPath;
            }
        } else {
            // Path doesn't end with /, assume it's a file path
            logger.debug("Path appears to be a file: {}", trimmedPath);
            return trimmedPath;
        }
    }
    
    @Override
    public String findFirstDataFileInDirectory(String s3DirectoryPath) {
        try {
            S3PathInfo pathInfo = parseS3Path(s3DirectoryPath);
            
            String directoryKey = pathInfo.objectKey;
            if (!directoryKey.endsWith("/")) {
                directoryKey += "/";
            }
            
            ListObjectsV2Request listRequest = ListObjectsV2Request.builder()
                .bucket(pathInfo.bucketName)
                .prefix(directoryKey)
                .maxKeys(awsEmrConfig.getS3MaxKeysPerRequest())
                .build();
            
            ListObjectsV2Response listResponse = s3Client.listObjectsV2(listRequest);
            
            for (S3Object s3Object : listResponse.contents()) {
                String objectKey = s3Object.key();
                String fileName = objectKey.substring(objectKey.lastIndexOf('/') + 1);
                
                if (objectKey.endsWith("/") || objectKey.equals(directoryKey)) {
                    continue;
                }
                
                if (isSparkMetadataFile(fileName)) {
                    logger.debug("Skipping Spark metadata file: {}", fileName);
                    continue;
                }
                
                String fullPath = String.format("s3://%s/%s", pathInfo.bucketName, objectKey);
                logger.info("Found data file in directory {}: {}", s3DirectoryPath, fullPath);
                return fullPath;
            }
            
            logger.warn("No data files found in directory: {}", s3DirectoryPath);
            return null;
            
        } catch (S3Exception e) {
            logger.error("Failed to list objects in S3 directory {}: {}", s3DirectoryPath, e.getMessage(), e);
            return null;
        } catch (Exception e) {
            logger.error("Unexpected error while listing S3 directory {}: {}", s3DirectoryPath, e.getMessage(), e);
            return null;
        }
    }

    private boolean isSparkMetadataFile(String fileName) {
        if (fileName == null || fileName.isEmpty()) {
            return true;
        }
        
        // Common Spark metadata files to ignore
        return fileName.equals("_SUCCESS") ||
               fileName.equals("_temporary") ||
               fileName.startsWith("_") ||
               fileName.endsWith(".crc") ||
               fileName.endsWith(".meta") ||
               fileName.endsWith(".index") ||
               fileName.endsWith(".summary") ||
               fileName.endsWith(".tmp");
    }
    
    /**
     * Parses S3 path to extract bucket name and object key
     * Supports both full S3 URIs (s3://bucket/path) and object keys (path)
     */
    private S3PathInfo parseS3Path(String s3Path) {
        if (s3Path == null || s3Path.trim().isEmpty()) {
            throw new IllegalArgumentException("S3 path cannot be null or empty");
        }
        
        String trimmedPath = s3Path.trim();
        
        if (trimmedPath.startsWith("s3://")) {
            String pathWithoutProtocol = trimmedPath.substring(5); // Remove "s3://"
            int firstSlashIndex = pathWithoutProtocol.indexOf('/');
            
            if (firstSlashIndex == -1) {
                throw new IllegalArgumentException("Invalid S3 URI format: " + s3Path);
            }
            
            String bucketName = pathWithoutProtocol.substring(0, firstSlashIndex);
            String objectKey = pathWithoutProtocol.substring(firstSlashIndex + 1);
            
            if (bucketName.isEmpty() || objectKey.isEmpty()) {
                throw new IllegalArgumentException("Invalid S3 URI format: " + s3Path);
            }
            
            return new S3PathInfo(bucketName, objectKey);
        } else {
            // Treat as object key and use configured bucket
            return new S3PathInfo(awsEmrConfig.getS3Bucket(), trimmedPath);
        }
    }
    
    /**
     * Helper class to hold parsed S3 path information
     */
    private static class S3PathInfo {
        final String bucketName;
        final String objectKey;
        
        S3PathInfo(String bucketName, String objectKey) {
            this.bucketName = bucketName;
            this.objectKey = objectKey;
        }
    }
    
    
    @Override
    public String getBucketName() {
        return awsEmrConfig.getS3Bucket();
    }
    
    @Override
    public long getS3FileSize(String s3Path) {
        try {
            S3PathInfo pathInfo = parseS3Path(s3Path);
            
            HeadObjectRequest headRequest = HeadObjectRequest.builder()
                .bucket(pathInfo.bucketName)
                .key(pathInfo.objectKey)
                .build();
            
            HeadObjectResponse headResponse = s3Client.headObject(headRequest);
            long fileSize = headResponse.contentLength();
            
            logger.debug("File size for {}: {} bytes", s3Path, fileSize);
            return fileSize;
            
        } catch (S3Exception e) {
            if (e.statusCode() == 404) {
                logger.warn("File not found: {}", s3Path);
                return -1;
            } else {
                logger.error("Error getting file size for {}: {}", s3Path, e.getMessage(), e);
                return -1;
            }
        } catch (Exception e) {
            logger.error("Unexpected error getting file size for {}: {}", s3Path, e.getMessage(), e);
            return -1;
        }
    }
    
    @Override
    public String uploadLocalFile(String localFilePath, String s3ObjectKey) {
        try {
            java.io.File localFile = new java.io.File(localFilePath);
            if (!localFile.exists()) {
                throw new RuntimeException("Local file does not exist: " + localFilePath);
            }
            
            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(awsEmrConfig.getS3Bucket())
                .key(s3ObjectKey)
                .contentType("text/csv")
                .build();
            
            s3Client.putObject(putObjectRequest, RequestBody.fromFile(localFile));
            
            String fullS3Path = String.format("s3://%s/%s", awsEmrConfig.getS3Bucket(), s3ObjectKey);
            logger.info("Successfully uploaded local file {} to S3: {}", localFilePath, fullS3Path);
            return s3ObjectKey;
            
        } catch (S3Exception e) {
            logger.error("Failed to upload local file {} to S3: {}", localFilePath, e.getMessage(), e);
            throw new RuntimeException("Failed to upload local file to S3", e);
        } catch (Exception e) {
            logger.error("Unexpected error while uploading local file {} to S3: {}", localFilePath, e.getMessage(), e);
            throw new RuntimeException("Unexpected error while uploading local file to S3", e);
        }
    }
    
    @Override
    public void close() {
        if (s3Client != null) {
            s3Client.close();
            logger.info("S3 client closed successfully");
        }
    }
}
