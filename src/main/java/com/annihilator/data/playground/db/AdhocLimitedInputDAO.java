package com.annihilator.data.playground.db;

import com.annihilator.data.playground.model.LimitedRunRequest;
import java.sql.SQLException;

public interface AdhocLimitedInputDAO {

  void createAdhocLimitedInput(String run_id, String playground_id, LimitedRunRequest request)
      throws SQLException;

  LimitedRunRequest getAdhocLimitedInputByRunId(String playgroundId) throws SQLException;
}
