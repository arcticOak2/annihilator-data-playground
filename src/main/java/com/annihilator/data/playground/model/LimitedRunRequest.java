package com.annihilator.data.playground.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class LimitedRunRequest {

    String playgroundId;

    Map<String, Boolean> tasksToRun;
}
