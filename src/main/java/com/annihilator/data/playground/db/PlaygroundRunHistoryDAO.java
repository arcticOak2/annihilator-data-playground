package com.annihilator.data.playground.db;

import com.annihilator.data.playground.model.Status;
import java.sql.SQLException;
import java.util.Map;
import java.util.UUID;

public interface PlaygroundRunHistoryDAO {

  void completePlaygroundAndInsertHistory(
      UUID playgroundId,
      Status currentStatus,
      long endTime,
      int successCount,
      int failureCount,
      Status finalStatus)
      throws SQLException;

  Map<String, Object> getRunHistoryByPlaygroundId(UUID playgroundId, int limit) throws SQLException;
}
