package com.annihilator.data.playground.model;

import java.util.UUID;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class Playground {

  private UUID id;

  private String name;

  private long createdAt;

  private long modifiedAt;

  private long lastExecutedAt;

  private String userId;

  private String cronExpression;

  private Status currentStatus;

  private Status lastRunStatus;

  private long lastRunEndTime;

  private int lastRunFailureCount;

  private int lastRunSuccessCount;

  private UUID correlationId;
}
