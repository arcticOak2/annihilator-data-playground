package com.annihilator.data.playground.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;

public class ConcurrencyConfig {

    @Min(value = 1, message = "Adhoc thread pool size must be at least 1")
    @Max(value = 1000, message = "Adhoc thread pool size cannot exceed 1000")
    private int adHocThreadPoolSize;

    @Min(value = 1, message = "Scheduled thread pool size must be at least 1")
    @Max(value = 1000, message = "Scheduled thread pool size cannot exceed 1000")
    private int scheduledThreadPoolSize;

    @Min(value = 1000, message = "Scheduler sleep time must be at least 1000ms (1 second)")
    @Max(value = 3600000, message = "Scheduler sleep time cannot exceed 3600000ms (1 hour)")
    private long schedulerSleepTime;

    @Min(value = 60000, message = "Playground execution grace period must be at least 60000ms (1 minute)")
    @Max(value = 1800000, message = "Playground execution grace period cannot exceed 1800000ms (30 minutes)")
    private long playgroundExecutionGracePeriod;

    @Min(value = 60000, message = "Playground max execution frequency must be at least 60000ms (1 minute)")
    @Max(value = 3600000, message = "Playground max execution frequency cannot exceed 3600000ms (1 hour)")
    private long playgroundMaxExecutionFrequency;

    @JsonProperty("playground_max_execution_frequency")
    public long getPlaygroundMaxExecutionFrequency() {
        return playgroundMaxExecutionFrequency;
    }

    @JsonProperty("playground_execution_grace_period")
    public long getPlaygroundExecutionGracePeriod() {
        return playgroundExecutionGracePeriod;
    }

    @JsonProperty("adhoc_threadpool_size")
    public int getAdHocThreadPoolSize() {
        return adHocThreadPoolSize;
    }

    @JsonProperty("scheduled_threadpool_size")
    public int getScheduledThreadPoolSize() {
        return scheduledThreadPoolSize;
    }

    @JsonProperty("scheduler_sleep_time")
    public long getSchedulerSleepTime() {
        return schedulerSleepTime;
    }

    public void setPlaygroundExecutionGracePeriod(long playgroundExecutionGracePeriod) {
        this.playgroundExecutionGracePeriod = playgroundExecutionGracePeriod;
    }

    public void setAdHocThreadPoolSize(int adHocThreadPoolSize) {
        this.adHocThreadPoolSize = adHocThreadPoolSize;
    }

    public void setScheduledThreadPoolSize(int scheduledThreadPoolSize) {
        this.scheduledThreadPoolSize = scheduledThreadPoolSize;
    }

    public void setSchedulerSleepTime(long schedulerSleepTime) {
        this.schedulerSleepTime = schedulerSleepTime;
    }

    public void setPlaygroundMaxExecutionFrequency(long playgroundMaxExecutionFrequency) {
        this.playgroundMaxExecutionFrequency = playgroundMaxExecutionFrequency;
    }
}
