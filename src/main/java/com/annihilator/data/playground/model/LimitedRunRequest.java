package com.annihilator.data.playground.model;

import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class LimitedRunRequest {

  String playgroundId;

  Map<String, Boolean> tasksToRun;
}
