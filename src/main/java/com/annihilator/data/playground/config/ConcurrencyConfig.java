package com.annihilator.data.playground.config;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ConcurrencyConfig {

    private int adHocThreadPoolSize;

    private int scheduledThreadPoolSize;

    private long schedulerSleepTime;

    private long playgroundExecutionGracePeriod;

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
