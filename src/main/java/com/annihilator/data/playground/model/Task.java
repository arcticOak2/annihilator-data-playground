package com.annihilator.data.playground.model;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

@Data
@NoArgsConstructor
public class Task {

    private UUID id;

    private String name;

    private UUID playgroundId;

    private long createdAt;

    private long modifiedAt;

    private TaskType type;

    private String query;

    private String outputLocation;

    private String logPath;

    private String resultPreview;

    private Status status;

    private UUID parentId;

    private Status lastRunStatus;

    private UUID correlationId;

    private UUID lastCorrelationId;

    private String udfIds; // Comma-separated list of UDF IDs

    // Helper method to get UDF IDs as a list
    public List<String> getUdfIdList() {
        if (udfIds == null || udfIds.trim().isEmpty()) {
            return Arrays.asList();
        }
        return Arrays.asList(udfIds.split(","));
    }

    // Helper method to set UDF IDs from a list
    public void setUdfIdList(List<String> udfIdList) {
        if (udfIdList == null || udfIdList.isEmpty()) {
            this.udfIds = null;
        } else {
            this.udfIds = String.join(",", udfIdList);
        }
    }
}
