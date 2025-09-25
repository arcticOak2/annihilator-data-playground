package com.annihilator.data.playground.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Reconciliation {

    UUID reconciliationId;

    String playgroundId;

    String leftTableId;

    String rightTableId;

    String mapping;

    long createdAt;

    long updatedAt;
}
