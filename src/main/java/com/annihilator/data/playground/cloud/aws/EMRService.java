package com.annihilator.data.playground.cloud.aws;

import com.annihilator.data.playground.config.AWSEmrConfig;
import com.annihilator.data.playground.db.TaskDAO;
import com.annihilator.data.playground.db.UDFDAO;
import com.annihilator.data.playground.model.StepResult;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudformation.CloudFormationClient;
import software.amazon.awssdk.services.emr.EmrClient;
import java.util.concurrent.CompletableFuture;

public interface EMRService {

    CompletableFuture<StepResult> submitTaskAndWait(String playgroundId, String taskId, String content, String taskType);

    static EMRService getInstance(AWSEmrConfig awsEmr, UDFDAO udfDAO, TaskDAO taskDAO) {

        String accessKeyId = awsEmr.getAccessKey();
        String secretAccessKey = awsEmr.getSecretKey();

        AwsBasicCredentials awsCredentials = AwsBasicCredentials.create(accessKeyId, secretAccessKey);
        StaticCredentialsProvider credentialsProvider = StaticCredentialsProvider.create(awsCredentials);

        CloudFormationClient cloudFormationClient = CloudFormationClient.builder()
                .region(Region.of(awsEmr.getRegion()))
                .credentialsProvider(credentialsProvider)
                .build();

        EmrClient emrClient = EmrClient.builder()
                .region(Region.of(awsEmr.getRegion()))
                .credentialsProvider(credentialsProvider)
                .build();

        S3Service s3Service = S3Service.getInstance(awsEmr);

        EMRService emrService = new EMRServiceImpl(
                awsEmr,
                cloudFormationClient,
                emrClient,
                s3Service,
                udfDAO,
                taskDAO
        );

        return emrService;
    }

    void close();
}