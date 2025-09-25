package com.annihilator.data.playground.cloud.aws;

import com.annihilator.data.playground.config.AWSEmrConfig;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.List;
import java.util.function.Consumer;

public interface S3Service {
    
    static S3Service getInstance(AWSEmrConfig awsConfig) {
        AwsBasicCredentials awsCredentials = AwsBasicCredentials.create(
            awsConfig.getAccessKey(),
            awsConfig.getSecretKey()
        );
        
        S3Client s3Client = S3Client.builder()
            .region(Region.of(awsConfig.getRegion()))
            .credentialsProvider(StaticCredentialsProvider.create(awsCredentials))
            .build();
        
        return new S3ServiceImpl(s3Client, awsConfig);
    }
    
    /**
     * Writes query text/script to S3 as a text file
     * @param queryText The query text/script to write (SQL, Python, Scala, etc.)
     * @param fileName The name of the file to create in S3 (include extension if needed)
     * @return The S3 object key (path) where the file was saved
     * @note Files are saved to: s3://bucket/{pathPrefix}/query/YYYY-MM-DD/fileName
     */
    String writeQueryToS3(String queryText, String fileName);
    
    /**
     * Writes reconciliation output data to S3 as a text file
     * @param data The data to write (JSON, CSV, etc.)
     * @param fileName The name of the file to create in S3 (include extension if needed)
     * @return The S3 object key (path) where the file was saved
     * @note Files are saved to: s3://bucket/{pathPrefix}/reconciliation/YYYY-MM-DD/fileName
     */
    String writeReconciliationOutput(String data, String fileName);
    
    /**
     * Reads a preview of a file from S3 (first few lines)
     * @param s3Path The full S3 path (e.g., s3://bucket-name/path/to/file.txt) or S3 object key
     * @param maxLines Maximum number of lines to read for preview (default: 100)
     * @return List of file lines as strings
     */
    List<String> readOutputPreview(String s3Path);
    
    /**
     * Reads a preview of a file from S3 with default line limit
     * @param s3Path The full S3 path (e.g., s3://bucket-name/path/to/file.txt) or S3 object key
     * @return List of file lines as strings (first 100 lines)
     */
    
    /**
     * Lists files in an S3 directory and returns the first data file found (ignores Spark metadata files)
     * @param s3DirectoryPath The full S3 directory path (e.g., s3://bucket-name/path/to/directory/)
     * @return The full S3 path to the first data file found in the directory, or null if no data files found
     */
    String findFirstDataFileInDirectory(String s3DirectoryPath);
    
    /**
     * Reads a file from S3 line by line, processing each line with the provided consumer function.
     * This method streams the file content without loading everything into memory at once.
     * @param s3Path The full S3 path (e.g., s3://bucket-name/path/to/file.txt) or S3 object key
     * @param lineProcessor Consumer function that processes each line as it's read
     * @throws RuntimeException if there's an error reading from S3
     */
    void readFileLineByLine(String s3Path, Consumer<String> lineProcessor);
    
    /**
     * Gets the S3 bucket name used by this service
     * @return The S3 bucket name
     */
    String getBucketName();
    
    /**
     * Gets the file size of an S3 object
     * @param s3Path The full S3 path (e.g., s3://bucket-name/path/to/file.txt) or S3 object key
     * @return The file size in bytes, or -1 if the file doesn't exist or there's an error
     */
    long getS3FileSize(String s3Path);
    
    /**
     * Uploads a local file to S3
     * @param localFilePath The path to the local file to upload
     * @param s3ObjectKey The S3 object key (path) where the file should be stored
     * @return The S3 object key where the file was uploaded
     */
    String uploadLocalFile(String localFilePath, String s3ObjectKey);
    
    /**
     * Closes the S3 client and releases resources
     */
    void close();
}
