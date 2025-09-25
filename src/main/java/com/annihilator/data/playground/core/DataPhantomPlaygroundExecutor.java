package com.annihilator.data.playground.core;

import com.annihilator.data.playground.cloud.aws.EMRService;
import com.annihilator.data.playground.connector.MySQLConnector;
import com.annihilator.data.playground.db.AdhocLimitedInputDAO;
import com.annihilator.data.playground.db.PlaygroundDAO;
import com.annihilator.data.playground.db.PlaygroundRunHistoryDAO;
import com.annihilator.data.playground.db.TaskDAO;
import com.annihilator.data.playground.model.*;
import com.annihilator.data.playground.reconsilation.DataPhantomReconciliationManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

public class DataPhantomPlaygroundExecutor implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(DataPhantomPlaygroundExecutor.class);

    private final Playground playground;
    private final TaskDAO taskDAO;
    private final EMRService emrService;
    private final PlaygroundDAO playgroundDAO;
    private final PlaygroundRunHistoryDAO historyDAO;
    private final AdhocLimitedInputDAO adhocLimitedInputDAO;
    private final PlaygroundExecutionType executionType;
    private final Set<String> cancelPlaygroundRequestSet;
    private final boolean limitedRun;
    private final Map<String, Boolean> selectionMap;
    private final DataPhantomReconciliationManager reconciliationManager;
    private final MySQLConnector mySQLConnector;

    private int successCount = 0;

    private int failureCount = 0;

    private int skippedCount = 0;

    private boolean isCancelled = false;

    public DataPhantomPlaygroundExecutor(Playground playground, TaskDAO taskDAO, PlaygroundDAO playgroundDAO, PlaygroundRunHistoryDAO historyDAO, AdhocLimitedInputDAO adhocLimitedInputDAO, EMRService emrService, PlaygroundExecutionType executionType, DataPhantomReconciliationManager reconciliationManager, Set<String> cancelPlaygroundRequestSet, MySQLConnector mySQLConnector, boolean limitedRun, Map<String, Boolean> selectionMap) {
        this.playground = playground;
        this.taskDAO = taskDAO;
        this.emrService = emrService;
        this.playgroundDAO = playgroundDAO;
        this.historyDAO = historyDAO;
        this.adhocLimitedInputDAO = adhocLimitedInputDAO;
        this.executionType = executionType;
        this.cancelPlaygroundRequestSet = cancelPlaygroundRequestSet;
        this.limitedRun = limitedRun;
        this.selectionMap = selectionMap;
        this.reconciliationManager = reconciliationManager;
        this.mySQLConnector = mySQLConnector;
    }

    @Override
    public void run() {

        try {

            Playground temp = playgroundDAO.getPlaygroundById(playground.getId());

            if (temp == null) {
                logger.warn("Playground with ID {} not found. Exiting execution.", playground.getId());
                return;
            }

            if (temp.getCurrentStatus() == Status.RUNNING &&
                    (executionType == PlaygroundExecutionType.SCHEDULED || executionType == PlaygroundExecutionType.AD_HOC)) {

                logger.info("Playground {} is already running. Skipping this execution.", playground.getName());
                return;
            }

            List<Task> tasks;

            tasks = taskDAO.findTasksByPlaygroundRecursively(playground.getId());

            if (tasks == null || tasks.isEmpty()) {
                logger.info("No tasks found for playground: {}", playground.getName());
                return;
            }

            UUID correlationId = executionType == PlaygroundExecutionType.RECOVERY ? playground.getCorrelationId() : UUID.randomUUID();

            if (executionType != PlaygroundExecutionType.RECOVERY && limitedRun) {

                adhocLimitedInputDAO.createAdhocLimitedInput(
                        correlationId.toString(),
                        playground.getId().toString(),
                        new LimitedRunRequest(playground.getId().toString(), selectionMap)
                );
            }

            DAGExecutionQueue dagExecutionQueue = buildLevels(tasks);

            if (executionType != PlaygroundExecutionType.RECOVERY) {
                updatePlaygroundMeta(dagExecutionQueue.queue, correlationId);
            }

            processTasks(dagExecutionQueue, correlationId);

            reconciliationManager.startReconciliation(playground.getId());
            updatePlaygroundMetaAfterCompletion();
        } catch (InterruptedException | SQLException e) {
            logger.error("Error executing playground {}: {}", playground.getName(), e.getMessage(), e);
            throw new RuntimeException(e);
        } finally {
            cancelPlaygroundRequestSet.remove(playground.getId().toString());
            logger.info("Playground {} execution completed. Success: {}, Failed: {}, Skipped: {}", playground.getName(), successCount, failureCount, skippedCount);
        }
    }

    private void updatePlaygroundMetaAfterCompletion() {
        try {
            Status finalStatus = Status.SUCCESS;

            if (isCancelled) {
                finalStatus = Status.CANCELLED;
            } else if (successCount == 0) {
                finalStatus = Status.FAILED;
            } else if (failureCount > 0) {
                finalStatus = Status.PARTIAL_SUCCESS;
            }

            historyDAO.completePlaygroundAndInsertHistory(
                    playground.getId(),
                    Status.IDLE,
                    System.currentTimeMillis(),
                    successCount,
                    failureCount,
                    finalStatus
            );

        } catch (SQLException e) {
            logger.error("Error updating playground completion and the run history for {}: {}", playground.getName(), e.getMessage(), e);
        }
    }

    private void updatePlaygroundMeta(Queue<Task> rootTasks, UUID correlationId) {

        if (!rootTasks.isEmpty()) {
            logger.info("Total {} root tasks found for playground: {}", rootTasks.size(), playground.getName());

            try {
                playgroundDAO.updatePlaygroundStart(
                        playground.getId(),
                        correlationId,
                        System.currentTimeMillis(),
                        Status.RUNNING
                );
            } catch (SQLException e) {
                logger.error("Error updating playground start for {}: {}", playground.getName(), e.getMessage(), e);
            }
        }
    }

    private void updateTaskStatus(UUID correlationId, Task task, Status status, StepResult stepResult) {

        try {

            if (status == Status.RUNNING) {
                taskDAO.updateTaskStatus(task.getId(), correlationId, status);
            } else {

                String outputPath = stepResult != null ? stepResult.getOutputPath() : null;
                String logPath = stepResult != null ? stepResult.getLogPath() : null;

                taskDAO.updateTaskCompletion(task.getId(), Status.IDLE, status, outputPath, logPath, correlationId);
            }
        } catch (SQLException e) {

            logger.error("Error updating status for task {}: {}", task.getName(), e.getMessage(), e);
        }
    }

    private void enqueueChildrenTasks(Task task, Queue<Task> queue, Map<String, Task> taskMap, Map<String, List<String>> parentChildrenMap) {

        List<String> children = parentChildrenMap.getOrDefault(task.getId().toString(), new ArrayList<>());

        for (String child : children) {

            logger.info("Enqueuing child task: {}", taskMap.get(child).getName());

            queue.add(taskMap.get(child));
        }
    }

    private boolean skipTask(Task task, PlaygroundExecutionType executionType, UUID correlationId, Queue<Task> queue, Map<String, Task> taskMap, Map<String, List<String>> parentChildrenMap) {

        if (limitedRun && !selectionMap.getOrDefault(task.getId().toString(), false)) {

            logger.info("Skipping task {} (ID: {}) as it's not in the selection map for limited run.", task.getName(), task.getId());

            skippedCount++;

            updateTaskStatus(correlationId, task, Status.SKIPPED, null);

            enqueueChildrenTasks(task, queue, taskMap, parentChildrenMap);

            return true;
        } else if (Objects.nonNull(task.getLastCorrelationId()) && task.getLastCorrelationId().equals(task.getCorrelationId()) &&
                task.getCorrelationId().equals(correlationId) &&
                executionType == PlaygroundExecutionType.RECOVERY) {

            logger.info("Skipping already completed task: {} (ID: {})", task.getName(), task.getId());

            if (task.getLastRunStatus() == Status.SUCCESS) {
                successCount++;
            } else {
                failureCount++;
            }

            enqueueChildrenTasks(task, queue, taskMap, parentChildrenMap);

            return true;
        }

        return false;
    }

    private void processTasks(DAGExecutionQueue dagExecutionQueue, UUID correlationId) throws InterruptedException {

        Queue<Task> queue = dagExecutionQueue.queue;
        Map<String, Task> taskMap = dagExecutionQueue.taskMap;
        Map<String, List<String>> parentChildrenMap = dagExecutionQueue.parentChildrenMap;

        Set<String> runningTasks = new HashSet<>();

        Map<String, CompletableFuture<StepResult>> futureMap = new HashMap<>();
        List<CompletableFuture<StepResult>> allFutures = new ArrayList<>();

        while (!queue.isEmpty() || !runningTasks.isEmpty()) {

            isCancelled = cancelPlaygroundRequestSet.contains(playground.getId().toString());

            if (isCancelled) {

                for (String taskId: futureMap.keySet()) {

                    CompletableFuture<StepResult> futureTask = futureMap.get(taskId);
                    runningTasks.remove(taskId);

                    if (futureTask.isDone()) {

                        continue;
                    }

                    futureTask.cancel(true);

                    updateTaskStatus(UUID.fromString(taskId), taskMap.get(taskId), Status.CANCELLED, null);

                    for (String childId : parentChildrenMap.getOrDefault(taskId, new ArrayList<>())) {

                        if (futureMap.containsKey(childId)) {
                            continue;
                        }

                        queue.add(taskMap.get(childId));
                    }
                }
            }

            while (!queue.isEmpty()) {

                Task task = queue.poll();

                if (skipTask(task, executionType, correlationId, queue, taskMap, parentChildrenMap)) {
                    continue;
                }

                if (isCancelled) {

                    skippedCount++;

                    updateTaskStatus(correlationId, task, Status.SKIPPED, null);

                    updateChildrenTaskStatus(task, taskMap, parentChildrenMap, correlationId, Status.SKIPPED);

                    continue;
                }


                updateTaskStatus(correlationId, task, Status.RUNNING, null);

                logger.info("Submitting task: {} (ID: {}) of type: {}", task.getName(), task.getId(), task.getType());

                runningTasks.add(task.getId().toString());

                CompletableFuture<StepResult> future;

                if (task.getType() == TaskType.SQL) {
                    future = mySQLConnector.executeSQLTask(task);
                } else {

                    future = emrService.submitTaskAndWait(
                            task.getPlaygroundId().toString(),
                            task.getId().toString(),
                            task.getQuery(),
                            task.getType().name());
                }

                future = future.thenApply(stepResult -> {

                            Task completedTask = taskMap.get(stepResult.getQueryId());

                            if (isCancelled) {

                                logger.info("Playground {} was cancelled. Not processing EMR completion for task {} (ID: {}).",
                                        playground.getName(), completedTask.getName(), completedTask.getId());
                                return stepResult;
                            }

                            logger.info("Task {} completed with status: {}", completedTask.getName(), stepResult.isSuccess() ? "SUCCESS" : "FAILED");
                            runningTasks.remove(completedTask.getId().toString());

                            if (stepResult.isSuccess()) {
                                successCount++;
                                updateTaskStatus(correlationId, completedTask, Status.SUCCESS, stepResult);
                                List<String> children = parentChildrenMap.getOrDefault(completedTask.getId().toString(), new ArrayList<>());
                                for (String child : children) {
                                    logger.info("Enqueuing child task: {}", taskMap.get(child).getName());
                                    queue.add(taskMap.get(child));
                                }
                            } else {
                                failureCount++;
                                updateTaskStatus(correlationId, completedTask, Status.FAILED, stepResult);
                                updateChildrenTaskStatus(completedTask, taskMap, parentChildrenMap, correlationId, Status.UPSTREAM_FAILED);
                            }

                            return stepResult;
                        });

                futureMap.put(task.getId().toString(), future);

                allFutures.add(future);
            }

            Thread.sleep(1000);
        }

        try {

            CompletableFuture.allOf(allFutures.toArray(new CompletableFuture[0])).join();
        } catch (CompletionException e) {
            if (e.getCause() instanceof CancellationException) {
                logger.info("Playground execution was cancelled.");
            } else {
                logger.error("Error during task execution: {}", e.getMessage(), e);
            }
        }
    }

    private void updateChildrenTaskStatus(Task completedTask, Map<String, Task> taskMap, Map<String, List<String>> parentChildrenMap, UUID correlationId, Status status) {

        Queue<String> toProcess = new LinkedList<>();
        toProcess.add(completedTask.getId().toString());

        while (!toProcess.isEmpty()) {
            String currentTaskId = toProcess.poll();
            List<String> children = parentChildrenMap.getOrDefault(currentTaskId, new ArrayList<>());

            for (String childId : children) {
                Task childTask = taskMap.get(childId);
                updateTaskStatus(correlationId, childTask, status, null);
                skippedCount++;
                toProcess.add(childId);
            }
        }
    }

    public DAGExecutionQueue buildLevels(List<Task> tasks) {

        Map<String, Task> taskMap = new HashMap<>();
        Map<String, List<String>> parentChildrenMap = new HashMap<>();
        Queue<Task> rootTasks = new LinkedList<>();

        for (Task task : tasks) {
            String taskId = task.getId().toString();
            taskMap.put(taskId, task);

            if (task.getParentId() != null) {
                parentChildrenMap.computeIfAbsent(task.getParentId().toString(), k -> new ArrayList<>()).add(taskId);
            } else {
                rootTasks.add(task);
            }
        }

        return new DAGExecutionQueue(rootTasks, parentChildrenMap, taskMap);
    }

    private static class DAGExecutionQueue {

        Queue<Task> queue;

        Map<String, List<String>> parentChildrenMap;

        Map<String, Task> taskMap;

        DAGExecutionQueue(Queue<Task> queue, Map<String, List<String>> parentChildrenMap, Map<String, Task> taskMap) {

            this.queue = queue;
            this.parentChildrenMap = parentChildrenMap;
            this.taskMap = taskMap;
        }
    }
}
