package com.annihilator.data.playground.model;

public class ReconciliationMappingUpdateRequest {
    private String map;

    // Constructors
    public ReconciliationMappingUpdateRequest() {}

    public ReconciliationMappingUpdateRequest(String map) {
        this.map = map;
    }

    // Getters and setters
    public String getMap() { return map; }
    public void setMap(String map) { this.map = map; }

    @Override
    public String toString() {
        return String.format("ReconciliationMappingUpdateRequest{map='%s'}", map);
    }
}

