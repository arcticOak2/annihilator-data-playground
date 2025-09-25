package com.annihilator.data.playground.model;

public class ReconciliationMappingRequest {
    private String playgroundId;
    private String leftTableId;
    private String rightTableId;
    private String map;

    // Constructors
    public ReconciliationMappingRequest() {}

    public ReconciliationMappingRequest(String playgroundId, String leftTableId, String rightTableId, String map) {
        this.playgroundId = playgroundId;
        this.leftTableId = leftTableId;
        this.rightTableId = rightTableId;
        this.map = map;
    }

    // Getters and setters
    public String getPlaygroundId() { return playgroundId; }
    public void setPlaygroundId(String playgroundId) { this.playgroundId = playgroundId; }

    public String getLeftTableId() { return leftTableId; }
    public void setLeftTableId(String leftTableId) { this.leftTableId = leftTableId; }

    public String getRightTableId() { return rightTableId; }
    public void setRightTableId(String rightTableId) { this.rightTableId = rightTableId; }

    public String getMap() { return map; }
    public void setMap(String map) { this.map = map; }

    @Override
    public String toString() {
        return String.format("ReconciliationMappingRequest{playgroundId='%s', leftTableId='%s', rightTableId='%s', map='%s'}", 
                           playgroundId, leftTableId, rightTableId, map);
    }
}

