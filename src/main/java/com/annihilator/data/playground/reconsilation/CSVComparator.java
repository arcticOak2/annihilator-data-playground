package com.annihilator.data.playground.reconsilation;

import java.sql.SQLException;

public interface CSVComparator {

    void runAllReconciliation(String playgroundId) throws SQLException;
    void runReconciliation(String reconciliationId) throws SQLException;
}
