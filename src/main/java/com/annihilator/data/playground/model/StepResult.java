package com.annihilator.data.playground.model;

import software.amazon.awssdk.services.emr.model.StepState;

public class StepResult {
  private final String stepId;
  private final StepState status;
  private final String message;
  private final String outputPath;
  private final String logPath;
  private final String queryId;

  public StepResult(
      String stepId,
      StepState status,
      String message,
      String outputPath,
      String logPath,
      String queryId) {
    this.stepId = stepId;
    this.status = status;
    this.message = message;
    this.outputPath = outputPath;
    this.logPath = logPath;
    this.queryId = queryId;
  }

  public String getStepId() {
    return stepId;
  }

  public StepState getStatus() {
    return status;
  }

  public String getMessage() {
    return message;
  }

  public String getOutputPath() {
    return outputPath;
  }

  public String getLogPath() {
    return logPath;
  }

  public String getQueryId() {
    return queryId;
  }

  public boolean isSuccess() {
    return status == StepState.COMPLETED;
  }

  public boolean isFailure() {
    return status == StepState.FAILED || status == StepState.CANCELLED;
  }

  public boolean isTimeout() {
    return status == StepState.RUNNING && message.contains("Timeout");
  }

  public boolean hasOutput() {
    return outputPath != null && !outputPath.isEmpty();
  }

  public boolean hasLogs() {
    return logPath != null && !logPath.isEmpty();
  }

  @Override
  public String toString() {
    return String.format(
        "StepResult{stepId='%s', status=%s, message='%s', outputPath='%s', logPath='%s'}",
        stepId, status, message, outputPath, logPath);
  }
}
