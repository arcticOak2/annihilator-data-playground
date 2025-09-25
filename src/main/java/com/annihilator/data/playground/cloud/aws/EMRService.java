package com.annihilator.data.playground.cloud.aws;

import com.annihilator.data.playground.config.AWSEmrConfig;
import com.annihilator.data.playground.model.StepResult;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudformation.CloudFormationClient;
import software.amazon.awssdk.services.emr.EmrClient;
import java.util.concurrent.CompletableFuture;

public interface EMRService {

    CompletableFuture<StepResult> submitTaskAndWait(String playgroundId, String queryId, String content, String taskType);

    static EMRService getInstance(AWSEmrConfig awsEmr, com.annihilator.data.playground.db.UDFDAO udfDAO, com.annihilator.data.playground.db.TaskDAO taskDAO) {

        String accessKeyId = awsEmr.getAccessKey();
        String secretAccessKey = awsEmr.getSecretKey();
        String region = awsEmr.getRegion();
        String stackName = awsEmr.getStackName();
        String s3Bucket = awsEmr.getS3Bucket();
        String clusterLogicalId = awsEmr.getClusterLogicalId();

        AwsBasicCredentials awsCredentials = AwsBasicCredentials.create(accessKeyId, secretAccessKey);
        StaticCredentialsProvider credentialsProvider = StaticCredentialsProvider.create(awsCredentials);

        CloudFormationClient cloudFormationClient = CloudFormationClient.builder()
                .region(Region.of(region))
                .credentialsProvider(credentialsProvider)
                .build();

        EmrClient emrClient = EmrClient.builder()
                .region(Region.of(region))
                .credentialsProvider(credentialsProvider)
                .build();

        S3Service s3Service = S3Service.getInstance(awsEmr);

        EMRService emrService = new EMRServiceImpl(
                stackName,
                clusterLogicalId,
                cloudFormationClient,
                emrClient,
                s3Service,
                s3Bucket,
                awsEmr.getS3PathPrefix(),
                udfDAO,
                taskDAO
        );

        return emrService;
    }

    void close();
}