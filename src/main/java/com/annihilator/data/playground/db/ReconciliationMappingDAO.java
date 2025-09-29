package com.annihilator.data.playground.db;

import com.annihilator.data.playground.model.Reconciliation;
import java.sql.SQLException;
import java.util.List;

public interface ReconciliationMappingDAO {

  void createReconciliationMapping(
      String playgroundId, String leftTableId, String rightTableId, String map) throws SQLException;

  List<Reconciliation> findReconciliationMappingByPlaygroundId(String playgroundId)
      throws SQLException;

  Reconciliation findReconciliationMappingById(String reconciliationId) throws SQLException;

  void deleteReconciliationMappingById(String reconciliationId) throws SQLException;

  void updateReconciliationMapping(String reconciliationId, String map) throws SQLException;
}
