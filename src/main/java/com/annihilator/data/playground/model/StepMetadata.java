package com.annihilator.data.playground.model;

public class StepMetadata {
    private final String outputPath;
    private final String logPath;
    private final String playgroundId;
    private final String queryId;
    private final String uniqueId;
    private final String currentDate;
    private final String taskType;

    public StepMetadata(String outputPath, String logPath, String playgroundId, String queryId, String uniqueId, String currentDate) {
        this(outputPath, logPath, playgroundId, queryId, uniqueId, currentDate, null);
    }

    public StepMetadata(String outputPath, String logPath, String playgroundId, String queryId, String uniqueId, String currentDate, String taskType) {
        this.outputPath = outputPath;
        this.logPath = logPath;
        this.playgroundId = playgroundId;
        this.queryId = queryId;
        this.uniqueId = uniqueId;
        this.currentDate = currentDate;
        this.taskType = taskType;
    }

    public String getOutputPath() { return outputPath; }
    public String getLogPath() { return logPath; }
    public String getPlaygroundId() { return playgroundId; }
    public String getQueryId() { return queryId; }
    public String getTaskType() { return taskType; }
    public String getUniqueId() { return uniqueId; }
    public String getCurrentDate() { return currentDate; }
}
