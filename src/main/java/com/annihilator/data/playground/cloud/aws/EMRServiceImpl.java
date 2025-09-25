package com.annihilator.data.playground.cloud.aws;

import com.annihilator.data.playground.model.StepMetadata;
import com.annihilator.data.playground.model.StepResult;
import com.annihilator.data.playground.model.TaskType;
import com.annihilator.data.playground.model.Task;
import com.annihilator.data.playground.db.UDFDAO;
import com.annihilator.data.playground.db.TaskDAO;
import com.annihilator.data.playground.utility.PrestoScriptGenerator;
import com.annihilator.data.playground.utility.HiveScriptGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudformation.CloudFormationClient;
import software.amazon.awssdk.services.cloudformation.model.*;
import software.amazon.awssdk.services.emr.EmrClient;
import software.amazon.awssdk.services.emr.model.*;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

public class EMRServiceImpl implements EMRService {
    
    private static final Logger logger = LoggerFactory.getLogger(EMRServiceImpl.class);
    
    private final CloudFormationClient cloudFormationClient;
    private final EmrClient emrClient;
    private final S3Service s3Service;
    private final UDFDAO udfDAO;
    private final TaskDAO taskDAO;
    private final String stackName;
    private final String clusterLogicalId;
    private final String outputBucket;
    private final String pathPrefix;
    private final ExecutorService executorService;
    private String currentClusterId;
    
    private final java.util.concurrent.ConcurrentHashMap<String, StepMetadata> stepMetadata = new java.util.concurrent.ConcurrentHashMap<>();
    
    public EMRServiceImpl(String stackName, String clusterLogicalId, Region region, UDFDAO udfDAO, TaskDAO taskDAO) {
        this(stackName, clusterLogicalId, region, "default-output-bucket", "data-phantom", udfDAO, taskDAO);
    }
    
    public EMRServiceImpl(String stackName, String clusterLogicalId, Region region, String outputBucket, UDFDAO udfDAO, TaskDAO taskDAO) {
        this(stackName, clusterLogicalId, region, outputBucket, "data-phantom", udfDAO, taskDAO);
    }
    
    public EMRServiceImpl(String stackName, String clusterLogicalId, Region region, String outputBucket, String pathPrefix, UDFDAO udfDAO, TaskDAO taskDAO) {
        this.stackName = stackName;
        this.clusterLogicalId = clusterLogicalId;
        this.outputBucket = outputBucket;
        this.pathPrefix = pathPrefix != null ? pathPrefix : "data-phantom";
        this.udfDAO = udfDAO;
        this.taskDAO = taskDAO;
        this.cloudFormationClient = CloudFormationClient.builder().region(region).build();
        this.emrClient = EmrClient.builder().region(region).build();
        this.s3Service = null;
        this.executorService = Executors.newCachedThreadPool();
    }
    
    public EMRServiceImpl(String stackName, String clusterLogicalId, 
                         CloudFormationClient cloudFormationClient, EmrClient emrClient, String outputBucket, UDFDAO udfDAO, TaskDAO taskDAO) {
        this(stackName, clusterLogicalId, cloudFormationClient, emrClient, outputBucket, "data-phantom", udfDAO, taskDAO);
    }
    
    public EMRServiceImpl(String stackName, String clusterLogicalId, 
                         CloudFormationClient cloudFormationClient, EmrClient emrClient, String outputBucket, String pathPrefix, UDFDAO udfDAO, TaskDAO taskDAO) {
        this.stackName = stackName;
        this.clusterLogicalId = clusterLogicalId;
        this.outputBucket = outputBucket;
        this.pathPrefix = pathPrefix != null ? pathPrefix : "data-phantom";
        this.udfDAO = udfDAO;
        this.taskDAO = taskDAO;
        this.cloudFormationClient = cloudFormationClient;
        this.emrClient = emrClient;
        this.s3Service = null;
        this.executorService = Executors.newCachedThreadPool();
    }
    
    public EMRServiceImpl(String stackName, String clusterLogicalId, 
                         CloudFormationClient cloudFormationClient, EmrClient emrClient, 
                         S3Service s3Service, String outputBucket, UDFDAO udfDAO, TaskDAO taskDAO) {
        this(stackName, clusterLogicalId, cloudFormationClient, emrClient, s3Service, outputBucket, "data-phantom", udfDAO, taskDAO);
    }
    
    public EMRServiceImpl(String stackName, String clusterLogicalId, 
                         CloudFormationClient cloudFormationClient, EmrClient emrClient, 
                         S3Service s3Service, String outputBucket, String pathPrefix, UDFDAO udfDAO, TaskDAO taskDAO) {
        this.stackName = stackName;
        this.clusterLogicalId = clusterLogicalId;
        this.outputBucket = outputBucket;
        this.pathPrefix = pathPrefix != null ? pathPrefix : "data-phantom";
        this.udfDAO = udfDAO;
        this.taskDAO = taskDAO;
        this.cloudFormationClient = cloudFormationClient;
        this.emrClient = emrClient;
        this.s3Service = s3Service;
        this.executorService = Executors.newCachedThreadPool();
    }
    

    @Override
    public CompletableFuture<StepResult> submitTaskAndWait(String playgroundId, String queryId, String content, String taskType) {
        return CompletableFuture.supplyAsync(() -> {
            int maxRetries = 3;
            String lastStepId = null;
            String lastFailureReason = null;
            StepResult lastResult = null;
            
            logger.info("Starting task {} with {} retries", queryId, maxRetries);
            
            for (int attempt = 0; attempt <= maxRetries; attempt++) {
                try {
                    logger.info("Attempting task {} (attempt {}/{})", queryId, attempt + 1, maxRetries + 1);
                    
                    Task task = taskDAO.findTaskById(queryId);
                    if (task == null) {
                        throw new RuntimeException("Task not found: " + queryId);
                    }
                    
                    String stepId = submitTaskWithCustomOutput(task, playgroundId, queryId, taskType);
                    lastStepId = stepId;
                    
                    StepResult result = waitForStepCompletion(stepId, 30000);
                    lastResult = result;
                    
                    if (result.isSuccess()) {
                        logger.info("Task {} succeeded on attempt {}", queryId, attempt + 1);
                        return result;
                    } else {
                        lastFailureReason = result.getMessage();
                        logger.warn("Task {} failed on attempt {}: {}", queryId, attempt + 1, lastFailureReason);
                        
                        if (attempt < maxRetries) {
                            logger.info("Retrying task {} (attempt {}/{})", queryId, attempt + 2, maxRetries + 1);
                        }
                    }
                    
                } catch (Exception e) {
                    logger.error("Task {} failed on attempt {} with exception: {}", queryId, attempt + 1, e.getMessage(), e);
                    lastFailureReason = "Exception: " + e.getMessage();
                    
                    if (attempt < maxRetries) {
                        logger.info("Retrying task {} after exception (attempt {}/{})", queryId, attempt + 2, maxRetries + 1);
                    }
                }
            }
            
            logger.error("Task {} failed after {} attempts. Last failure: {}", queryId, maxRetries + 1, lastFailureReason);
            
            if (lastResult != null) {
                return new StepResult(lastStepId, StepState.FAILED, 
                    "Failed after " + (maxRetries + 1) + " attempts. Last failure: " + lastFailureReason, 
                    null, lastResult.getLogPath(), queryId);
            } else {
                return new StepResult(lastStepId, StepState.FAILED, 
                    "Failed after " + (maxRetries + 1) + " attempts. Last failure: " + lastFailureReason, 
                    null, null, queryId);
            }
            
        }, executorService);
    }


    private StepResult waitForStepCompletion(String stepId, long pollIntervalMs) throws InterruptedException {
        logger.info("Waiting for step {} to complete (polling every {}ms)", stepId, pollIntervalMs);
        
        while (true) {
            StepState status = getStepStatus(stepId);
            
            if (status == StepState.COMPLETED) {
                logger.info("Step {} completed successfully", stepId);
                StepMetadata metadata = stepMetadata.get(stepId);
                String outputPath = metadata != null ? metadata.getOutputPath() : getStepOutputPath(stepId);
                String logPath = metadata != null ? metadata.getLogPath() : null;
                
                if (metadata != null && TaskType.SPARK_SQL.name().equals(metadata.getTaskType())) {
                    String actualFilePath = s3Service.findFirstDataFileInDirectory(outputPath);
                    if (actualFilePath != null) {
                        logger.info("Updated Spark SQL output path from {} to {}", outputPath, actualFilePath);
                        outputPath = actualFilePath;
                        stepMetadata.put(stepId, new StepMetadata(actualFilePath, logPath, metadata.getPlaygroundId(), metadata.getQueryId(), metadata.getUniqueId(), metadata.getCurrentDate(), metadata.getTaskType()));
                    }
                }
                
                return new StepResult(stepId, status, "Step completed successfully", outputPath, logPath, metadata != null ? metadata.getQueryId() : null);
            } else if (status == StepState.FAILED || status == StepState.CANCELLED) {
                String reason = getStepFailureReason(stepId);
                logger.error("Step {} failed with status: {} - {}", stepId, status, reason);
                StepMetadata metadata = stepMetadata.get(stepId);
                String logPath = metadata != null ? metadata.getLogPath() : null;
                return new StepResult(stepId, status, reason, null, logPath, metadata != null ? metadata.getQueryId() : null);
            }
            
            Thread.sleep(pollIntervalMs);
        }
    }


    @Override
    public void close() {
        try {
            executorService.shutdown();

            if (cloudFormationClient != null) {
                cloudFormationClient.close();
            }

            if (emrClient != null) {
                emrClient.close();
            }
            
            if (s3Service != null) {
                s3Service.close();
            }
            
            stepMetadata.clear();
            currentClusterId = null;

            logger.info("EMR service closed successfully");

        } catch (Exception e) {
            logger.warn("Error closing EMR service: {}", e.getMessage());
        }
    }
    
    private String submitTaskWithCustomOutput(Task task, String playgroundId, String queryId, String taskType) {
        try {
            ensureClusterReady();

            String currentDate = java.time.LocalDate.now().toString();
            
            String uniqueId = queryId; // queryId is actually the task ID
            String timestamp = String.valueOf(System.currentTimeMillis());
            
            String outputPath = String.format("s3://%s/%s/%s/%s/%s.txt", 
                                             outputBucket, pathPrefix, currentDate, playgroundId, queryId);

            String tempFile = String.format("/tmp/hive-output-%s-%s-%s.txt", 
                                          uniqueId, playgroundId, queryId);
            
            StepConfig stepConfig;
            
            if (TaskType.PY_SPARK.name().equals(taskType)) {
                stepConfig = createSparkStepConfig(task.getQuery(), playgroundId, queryId, uniqueId, timestamp);
            } else if (TaskType.SPARK_SQL.name().equals(taskType)) {
                stepConfig = createSparkSQLStepConfig(task.getQuery(), playgroundId, queryId, uniqueId, timestamp, currentDate);
                outputPath = String.format("s3://%s/%s/sparksql-output/%s/%s/%s/%s/", 
                                         outputBucket, pathPrefix, currentDate, playgroundId, queryId, uniqueId);
            } else {
                stepConfig = createHivePrestoStepConfig(task, tempFile, outputPath, playgroundId, queryId, uniqueId, currentDate, taskType, timestamp);
            }

            AddJobFlowStepsRequest request = AddJobFlowStepsRequest.builder()
                .jobFlowId(currentClusterId)
                .steps(stepConfig)
                .build();

            AddJobFlowStepsResponse response = emrClient.addJobFlowSteps(request);
            String stepId = response.stepIds().get(0);

            String logPath = String.format("s3://%s/%s/logs/%s/%s-log-%s-%s-%s.log", 
                                         outputBucket, pathPrefix, currentDate, taskType.toLowerCase(), playgroundId, queryId, uniqueId);
            
            if (TaskType.PY_SPARK.name().equals(taskType)) {
                logPath = null;
                outputPath = null;
            } else if (TaskType.SPARK_SQL.name().equals(taskType)) {
                logPath = null;
            }
            
            stepMetadata.put(stepId, new StepMetadata(outputPath, logPath, playgroundId, queryId, uniqueId, currentDate, taskType));

            logger.info("Task submitted successfully with step ID: {} and output path: {}", stepId, outputPath);

            return stepId;

        } catch (Exception e) {
            logger.error("Failed to submit task with custom output", e);
            throw new RuntimeException("Failed to submit task with custom output", e);
        }
    }
    
    private String generateScriptFromTemplate(String query, String tempFile, String outputPath, 
                                            String playgroundId, String queryId, String uniqueId, String currentDate, String taskType) {
        return generateScriptFromTemplate(query, tempFile, outputPath, playgroundId, queryId, uniqueId, currentDate, taskType, pathPrefix);
    }
    
    private String generateScriptFromTemplate(String query, String tempFile, String outputPath, 
                                            String playgroundId, String queryId, String uniqueId, String currentDate, String taskType, String pathPrefix) {
        try {
            String template = loadTemplate(taskType);
            
            String timestamp = java.time.LocalDateTime.now().format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
            
            String script = template
                .replace("${timestamp}", timestamp)
                .replace("${playgroundId}", playgroundId != null ? playgroundId : "unknown")
                .replace("${queryId}", queryId != null ? queryId : "unknown")
                .replace("${uniqueId}", uniqueId)
                .replace("${bucket}", outputBucket)
                .replace("${date}", currentDate)
                .replace("${query}", query)
                .replace("${tempFile}", tempFile)
                .replace("${outputPath}", outputPath)
                .replace("${pathPrefix}", pathPrefix);
            
            logger.info("Generated script from template for playground: {}, query: {}, uniqueId: {}, taskType: {}", 
                       playgroundId, queryId, uniqueId, taskType);
            return script;
            
        } catch (Exception e) {
            logger.error("Failed to generate script from template", e);
            return String.format("hive -e \"%s\" > %s 2>&1 && aws s3 cp %s %s", query, tempFile, tempFile, outputPath);
        }
    }
    
    private String loadTemplate(String taskType) throws IOException {
        String templatePath;
        switch (taskType) {
            case "PRESTO":
                templatePath = "/presto-query-template.sh";
                break;
            case "SPARK_SQL":
                templatePath = "/sparksql-template.py";
                break;
            case "HIVE":
            default:
                templatePath = "/hive-query-template.sh";
                break;
        }
        
        try (InputStream inputStream = EMRServiceImpl.class.getResourceAsStream(templatePath)) {
            if (inputStream == null) {
                throw new IOException("Template file not found: " + templatePath);
            }
            
            byte[] bytes = inputStream.readAllBytes();
            return new String(bytes, StandardCharsets.UTF_8);
        }
    }
    
    private String generateSparkSQLScriptFromTemplate(String query, String playgroundId, String queryId, 
                                                     String uniqueId, String currentDate, String outputBucket) {
        return generateSparkSQLScriptFromTemplate(query, playgroundId, queryId, uniqueId, currentDate, outputBucket, pathPrefix);
    }
    
    private String generateSparkSQLScriptFromTemplate(String query, String playgroundId, String queryId, 
                                                     String uniqueId, String currentDate, String outputBucket, String pathPrefix) {
        try {
            String template = loadTemplate(TaskType.SPARK_SQL.name());
            
            String cleanedQuery = query.trim();
            if (cleanedQuery.endsWith(";")) {
                cleanedQuery = cleanedQuery.substring(0, cleanedQuery.length() - 1).trim();
                logger.debug("Removed trailing semicolon from SparkSQL query");
            }
            
            return template
                .replace("${timestamp}", java.time.LocalDateTime.now().toString())
                .replace("${playgroundId}", playgroundId)
                .replace("${queryId}", queryId)
                .replace("${uniqueId}", uniqueId)
                .replace("${query}", cleanedQuery)
                .replace("${currentDate}", currentDate)
                .replace("${outputBucket}", outputBucket)
                .replace("${pathPrefix}", pathPrefix);
        } catch (IOException e) {
            logger.error("Error generating SparkSQL script from template", e);
            String cleanedQuery = query.trim();
            if (cleanedQuery.endsWith(";")) {
                cleanedQuery = cleanedQuery.substring(0, cleanedQuery.length() - 1).trim();
            }
            return String.format("#!/usr/bin/env python3\n" +
                               "from pyspark.sql import SparkSession\n\n" +
                               "spark = SparkSession.builder.appName(\"DataPhantomSparkSQL\").enableHiveSupport().getOrCreate()\n" +
                               "try:\n" +
                               "    df = spark.sql(\"%s\")\n" +
                               "    df.show(1000, False)\n" +
                               "finally:\n" +
                               "    spark.stop()", cleanedQuery);
        }
    }
    
    private String getStepOutputPath(String stepId) {
        try {
            DescribeStepRequest request = DescribeStepRequest.builder()
                .clusterId(currentClusterId)
                .stepId(stepId)
                .build();
            
            DescribeStepResponse response = emrClient.describeStep(request);
            String stepName = response.step().name();
            
            if (stepName != null && (stepName.startsWith("HiveQuery-") || stepName.startsWith("SparkQuery-") || stepName.startsWith("SparkSQLQuery-") || stepName.startsWith("PrestoQuery-"))) {
                String remaining;
                if (stepName.startsWith("HiveQuery-")) {
                    remaining = stepName.substring("HiveQuery-".length());
                } else if (stepName.startsWith("SparkQuery-")) {
                    remaining = stepName.substring("SparkQuery-".length());
                } else if (stepName.startsWith("SparkSQLQuery-")) {
                    remaining = stepName.substring("SparkSQLQuery-".length());
                } else {
                    remaining = stepName.substring("PrestoQuery-".length());
                }
                
                if (remaining.contains("-")) {
                    String[] parts = remaining.split("-", 3);
                    if (parts.length >= 3) {
                        String timestamp = parts[0];
                        String playgroundId = parts[1];
                        String queryId = parts[2];
                        
                        String currentDate = java.time.LocalDate.now().toString();
                        
                        if (stepName.startsWith("SparkSQLQuery-")) {
                            String uniqueId = timestamp; // Use timestamp as uniqueId for reconstruction
                            String directoryPath = String.format("s3://%s/%s/sparksql-output/%s/%s/%s/%s/", 
                                                               outputBucket, pathPrefix, currentDate, playgroundId, queryId, uniqueId);
                            
                            String actualFilePath = s3Service.findFirstDataFileInDirectory(directoryPath);
                            return actualFilePath != null ? actualFilePath : directoryPath;
                        } else {
                            return String.format("s3://%s/%s/%s/%s/%s.txt", 
                                               outputBucket, pathPrefix, currentDate, playgroundId, queryId);
                        }
                    }
                }
                
                return String.format("s3://%s/%s/hive-query-%s.txt", 
                                   outputBucket, pathPrefix, remaining);
            }
            
            return null;
            
        } catch (Exception e) {
            logger.warn("Failed to get step output path for {}: {}", stepId, e.getMessage());
            return null;
        }
    }
    
    private StepConfig createSparkStepConfig(String query, String folderName, String queryName, String uniqueId, String timestamp) {
        String pythonS3Key = s3Service.writeQueryToS3(query, String.format("spark-script-%s-%s-%s.py", 
                                                                           folderName, queryName, uniqueId));
        String pythonS3Path = String.format("s3://%s/%s", outputBucket, pythonS3Key);
        
        return StepConfig.builder()
            .name("SparkQuery-" + timestamp + "-" + folderName + "-" + queryName)
            .actionOnFailure(ActionOnFailure.CONTINUE)
            .hadoopJarStep(HadoopJarStepConfig.builder()
                .jar("command-runner.jar")
                .args(Arrays.asList("spark-submit", 
                                   "--deploy-mode", "cluster",
                                   "--conf", "spark.hadoop.hive.enforce.bucketing=true",
                                   "--conf", "spark.hadoop.hive.enforce.sorting=true",
                                   pythonS3Path))
                .build())
            .build();
    }
    
    private StepConfig createSparkSQLStepConfig(String query, String folderName, String queryName, String uniqueId, String timestamp, String currentDate) {
        String scriptContent = generateSparkSQLScriptFromTemplate(query, folderName, queryName, uniqueId, currentDate, outputBucket);
        String pythonS3Key = s3Service.writeQueryToS3(scriptContent, String.format("sparksql-script-%s-%s-%s.py", 
                                                                                   folderName, queryName, uniqueId));
        String pythonS3Path = String.format("s3://%s/%s", outputBucket, pythonS3Key);
        
        return StepConfig.builder()
            .name("SparkSQLQuery-" + timestamp + "-" + folderName + "-" + queryName)
            .actionOnFailure(ActionOnFailure.CONTINUE)
            .hadoopJarStep(HadoopJarStepConfig.builder()
                .jar("command-runner.jar")
                .args(Arrays.asList("spark-submit", 
                                   "--deploy-mode", "cluster",
                                   "--conf", "spark.hadoop.hive.enforce.bucketing=true",
                                   "--conf", "spark.hadoop.hive.enforce.sorting=true",
                                   pythonS3Path))
                .build())
            .build();
    }
    
    private StepConfig createHivePrestoStepConfig(Task task, String tempFile, String outputPath, String folderName, 
                                                 String queryName, String uniqueId, String currentDate, String taskType, String timestamp) throws SQLException {
        String scriptContent;
        
        if (TaskType.PRESTO.name().equals(taskType)) {
            List<String> scriptLines = PrestoScriptGenerator.generatePrestoScript(
                task, 
                udfDAO, 
                folderName, 
                queryName, 
                uniqueId, 
                outputBucket, 
                pathPrefix,
                currentDate, 
                tempFile, 
                outputPath
            );
            scriptContent = String.join("\n", scriptLines);
            logger.info("Generated Presto script using PrestoScriptGenerator for task: {}", task.getId());
        } else if (TaskType.HIVE.name().equals(taskType)) {
            List<String> scriptLines = HiveScriptGenerator.generateHiveScript(
                task, 
                udfDAO, 
                folderName, 
                queryName, 
                uniqueId, 
                outputBucket, 
                pathPrefix,
                currentDate, 
                tempFile, 
                outputPath
            );
            scriptContent = String.join("\n", scriptLines);
            logger.info("Generated Hive script using HiveScriptGenerator for task: {}", task.getId());
        } else {
            throw new IllegalArgumentException("Unsupported task type: " + taskType);
        }
        
        String scriptS3Key = s3Service.writeQueryToS3(scriptContent, String.format("%s-script-%s-%s-%s.sh", 
                                                                                  taskType, folderName, queryName, uniqueId));
        String scriptS3Path = String.format("s3://%s/%s", outputBucket, scriptS3Key);
        
        String stepName = taskType.equals(TaskType.PRESTO.name()) ? "PrestoQuery" : "HiveQuery";
        return StepConfig.builder()
            .name(stepName + "-" + timestamp + "-" + folderName + "-" + queryName)
            .actionOnFailure(ActionOnFailure.CONTINUE)
            .hadoopJarStep(HadoopJarStepConfig.builder()
                .jar("command-runner.jar")
                .args(Arrays.asList("bash", "-c", 
                    String.format("aws s3 cp %s /tmp/script.sh && chmod +x /tmp/script.sh && /tmp/script.sh", scriptS3Path)))
                .build())
            .build();
    }

    
    private synchronized void ensureClusterReady() {
        try {
            if (currentClusterId != null && isClusterStillRunning(currentClusterId)) {
                logger.debug("Using stored cluster ID: {}", currentClusterId);
                return;
            }
            
            logger.info("Stored cluster ID {} is not alive, fetching any existing cluster ID", currentClusterId);
            String existingClusterId = fetchExistingClusterId();
            
            if (existingClusterId != null) {
                logger.info("Found existing cluster: {}, caching it", existingClusterId);
                currentClusterId = existingClusterId;
                return;
            }
            
            logger.info("No running cluster found, updating stack to create new cluster");
            updateStackWithNewRandom();
            waitForStackUpdateComplete();
            
            String newClusterId = getClusterIdFromStackResources();
            if (newClusterId != null) {
                waitForClusterReady(newClusterId);
                currentClusterId = newClusterId;
                logger.info("Successfully created and cached new cluster: {}", newClusterId);
            } else {
                throw new RuntimeException("Failed to get cluster ID after CloudFormation update");
            }

        } catch (Exception e) {
            logger.error("Failed to ensure cluster is ready", e);
            throw new RuntimeException("Failed to ensure cluster is ready", e);
        }
    }
    
    private String fetchExistingClusterId() {
        try {
            String clusterId = getClusterIdFromStackResources();
            
            if (clusterId != null) {
                ClusterState state = getClusterState(clusterId);
                
                if (state == ClusterState.WAITING || state == ClusterState.RUNNING) {
                    logger.info("Found ready cluster from CloudFormation: {} (state: {})", clusterId, state);
                    return clusterId;
                } else if (state == ClusterState.BOOTSTRAPPING || state == ClusterState.STARTING) {
                    logger.info("Found bootstrapping cluster from CloudFormation: {} (state: {}), waiting for it to be ready", clusterId, state);
                    waitForClusterReady(clusterId);
                    return clusterId;
                } else {
                    logger.info("Cluster {} from CloudFormation is in state: {}, will be recreated", clusterId, state);
                }
            }

            String existingClusterId = findExistingCluster();
            if (existingClusterId != null) {
                logger.info("Found existing cluster: {}", existingClusterId);
                return existingClusterId;
            }

            logger.info("No existing clusters found");
            return null;

        } catch (Exception e) {
            logger.error("Failed to fetch existing cluster ID", e);
            return null;
        }
    }
    
    private boolean isClusterStillRunning(String clusterId) {
        try {
            DescribeClusterRequest request = DescribeClusterRequest.builder()
                .clusterId(clusterId)
                .build();
            
            DescribeClusterResponse response = emrClient.describeCluster(request);
            ClusterState state = response.cluster().status().state();
            
            return state == ClusterState.WAITING || state == ClusterState.RUNNING;
            
        } catch (Exception e) {
            logger.warn("Failed to check cluster state for {}: {}", clusterId, e.getMessage());
            return false;
        }
    }
    
    
    private boolean isClusterReady(String clusterId) {
        try {
            DescribeClusterRequest request = DescribeClusterRequest.builder()
                .clusterId(clusterId)
                .build();
            
            DescribeClusterResponse response = emrClient.describeCluster(request);
            ClusterState state = response.cluster().status().state();
            
            if (state == ClusterState.TERMINATED || state == ClusterState.TERMINATED_WITH_ERRORS) {
                return false;
            }
            
            return state == ClusterState.WAITING || state == ClusterState.RUNNING;
            
        } catch (Exception e) {
            logger.warn("Failed to check cluster state for {}: {}", clusterId, e.getMessage());
            return false;
        }
    }
    
    private ClusterState getClusterState(String clusterId) {
        try {
            DescribeClusterRequest request = DescribeClusterRequest.builder()
                .clusterId(clusterId)
                .build();
            
            DescribeClusterResponse response = emrClient.describeCluster(request);
            return response.cluster().status().state();
            
        } catch (Exception e) {
            logger.warn("Failed to get cluster state for {}: {}", clusterId, e.getMessage());
            return ClusterState.UNKNOWN_TO_SDK_VERSION;
        }
    }
    
    private String getClusterIdFromStackResources() {
        try {
            ListStackResourcesRequest request = ListStackResourcesRequest.builder()
                .stackName(stackName)
                .build();
            
            ListStackResourcesResponse response = cloudFormationClient.listStackResources(request);
            
            for (StackResourceSummary resource : response.stackResourceSummaries()) {
                if ("AWS::EMR::Cluster".equals(resource.resourceType())) {
                    return resource.physicalResourceId();
                }
            }
            
            return null;
            
        } catch (Exception e) {
            logger.warn("Failed to get cluster ID from stack resources: {}", e.getMessage());
            return null;
        }
    }
    
    private String findExistingCluster() {
        try {
            ListClustersRequest request = ListClustersRequest.builder()
                .clusterStates(ClusterState.RUNNING, ClusterState.WAITING, ClusterState.STARTING, ClusterState.BOOTSTRAPPING)
                .build();
            
            ListClustersResponse response = emrClient.listClusters(request);
            
            if (response.clusters().isEmpty()) {
                return null;
            }
            
            String stackClusterId = getClusterIdFromStackResources();
            if (stackClusterId != null) {
                for (ClusterSummary cluster : response.clusters()) {
                    if (cluster.id().equals(stackClusterId)) {
                        logger.info("Found existing cluster from CloudFormation stack: {} (state: {})", cluster.id(), cluster.status().state());
                        return cluster.id();
                    }
                }
            }
            
            for (ClusterSummary cluster : response.clusters()) {
                if (cluster.name() != null && cluster.name().contains(clusterLogicalId)) {
                    logger.info("Found existing cluster with matching name: {} (state: {})", cluster.id(), cluster.status().state());
                    return cluster.id();
                }
            }
            
            ClusterSummary firstCluster = response.clusters().get(0);
            logger.info("Using first available existing cluster: {} (state: {})", firstCluster.id(), firstCluster.status().state());
            return firstCluster.id();
            
        } catch (Exception e) {
            logger.warn("Failed to find existing clusters: {}", e.getMessage());
            return null;
        }
    }
    
    private void updateStackWithNewRandom() {
        try {
            int newRandom = ThreadLocalRandom.current().nextInt(1, Integer.MAX_VALUE);
            
            UpdateStackRequest request = UpdateStackRequest.builder()
                .stackName(stackName)
                .usePreviousTemplate(true)
                .parameters(
                    Parameter.builder()
                        .parameterKey("Random")
                        .parameterValue(String.valueOf(newRandom))
                        .build()
                )
                .build();
            
            cloudFormationClient.updateStack(request);
            
        } catch (CloudFormationException e) {
            if (e.getMessage().contains("No updates are to be performed")) {
                logger.warn("Random parameter change didn't trigger resource updates - cluster won't be recreated");
                return;
            } else if (e.getMessage().contains("UPDATE_IN_PROGRESS")) {
                waitForStackUpdateComplete();
                updateStackWithNewRandom();
                return;
            }
            logger.error("Failed to update CloudFormation stack", e);
            throw new RuntimeException("Failed to update CloudFormation stack", e);
        } catch (Exception e) {
            logger.error("Failed to update CloudFormation stack", e);
            throw new RuntimeException("Failed to update CloudFormation stack", e);
        }
    }
    
    
    private void waitForStackUpdateComplete() {
        try {
            int maxAttempts = 60;
            int attempt = 0;
            
            while (attempt < maxAttempts) {
                try {
                    DescribeStacksRequest request = DescribeStacksRequest.builder()
                        .stackName(stackName)
                        .build();
                    
                    DescribeStacksResponse response = cloudFormationClient.describeStacks(request);
                    if (!response.stacks().isEmpty()) {
                        StackStatus status = response.stacks().get(0).stackStatus();
                        if (status == StackStatus.UPDATE_COMPLETE) {
                            return;
                        } else if (status == StackStatus.UPDATE_ROLLBACK_COMPLETE || 
                                  status == StackStatus.UPDATE_ROLLBACK_FAILED) {
                            throw new RuntimeException("Stack update failed with status: " + status);
                        }
                    }
                    
                    Thread.sleep(30000);
                    attempt++;
                    
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted while waiting for stack update", e);
                }
            }
            
            throw new RuntimeException("Stack update did not complete within the timeout period");
            
        } catch (Exception e) {
            logger.error("Failed to wait for stack update completion", e);
            throw new RuntimeException("Failed to wait for stack update completion", e);
        }
    }
    
    private void waitForClusterReady(String clusterId) {
        try {
            int maxAttempts = 60;
            int attempt = 0;
            
            while (attempt < maxAttempts) {
                if (isClusterReady(clusterId)) {
                    return;
                }
                
                Thread.sleep(30000); // Wait 30 seconds
                attempt++;
            }
            
            throw new RuntimeException("Cluster did not become ready within the timeout period");
            
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while waiting for cluster to be ready", e);
        } catch (Exception e) {
            logger.error("Failed to wait for cluster to be ready", e);
            throw new RuntimeException("Failed to wait for cluster to be ready", e);
        }
    }
    
    private StepState getStepStatus(String stepId) {
        try {
            DescribeStepRequest request = DescribeStepRequest.builder()
                .clusterId(currentClusterId)
                .stepId(stepId)
                .build();
            
            DescribeStepResponse response = emrClient.describeStep(request);
            return response.step().status().state();
            
        } catch (Exception e) {
            logger.warn("Failed to get step status for {}: {}", stepId, e.getMessage());
            return StepState.PENDING;
        }
    }
    
    private String getStepFailureReason(String stepId) {
        try {
            DescribeStepRequest request = DescribeStepRequest.builder()
                .clusterId(currentClusterId)
                .stepId(stepId)
                .build();
            
            DescribeStepResponse response = emrClient.describeStep(request);
            return response.step().status().stateChangeReason().message();
            
        } catch (Exception e) {
            logger.warn("Failed to get step failure reason for {}: {}", stepId, e.getMessage());
            return "Unable to retrieve failure reason: " + e.getMessage();
        }
    }
}