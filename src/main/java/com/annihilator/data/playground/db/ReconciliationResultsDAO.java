package com.annihilator.data.playground.db;

import com.annihilator.data.playground.model.CSVComparisonResult;
import com.annihilator.data.playground.model.ReconciliationResultResponse;

import java.sql.SQLException;

public interface ReconciliationResultsDAO {

    void upsertReconciliationResult(String reconciliationId, CSVComparisonResult result, String status, String matchType) throws SQLException;
    
    ReconciliationResultResponse getReconciliationResult(String reconciliationId) throws SQLException;
}
