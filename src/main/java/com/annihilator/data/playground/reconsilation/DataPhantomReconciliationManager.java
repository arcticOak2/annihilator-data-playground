package com.annihilator.data.playground.reconsilation;

import com.annihilator.data.playground.db.TaskDAO;
import com.annihilator.data.playground.model.Task;
import com.annihilator.data.playground.utility.SQLQueryFieldExtractor;
import org.slf4j.Logger;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

public class DataPhantomReconciliationManager {

    private static final Logger LOGGER = org.slf4j.LoggerFactory.getLogger(DataPhantomReconciliationManager.class);

    private final TaskDAO taskDAO;
    private final CSVComparator csvComparator;

    public DataPhantomReconciliationManager(TaskDAO taskDAO, CSVComparator csvComparator) {

        this.taskDAO = taskDAO;
        this.csvComparator = csvComparator;
    }

    public List<String> getSelectedFields(String taskId) {

        Task task = null;
        try {
            task = taskDAO.findTaskById(taskId);
        } catch (Exception e) {
            LOGGER.error("Error retrieving selected fields for task {}: {}", taskId, e.getMessage());
        }

        if (Objects.isNull(task)) {
            LOGGER.error("No task found with ID: {}", taskId);
            return new ArrayList<>();
        }

        String sql = task.getQuery();

        return SQLQueryFieldExtractor.extractSelectedFields(sql);
    }

    public void startReconciliation(UUID playgroundId) {

        try {
            csvComparator.runAllReconciliation(playgroundId.toString());
        } catch (SQLException e) {
            LOGGER.error("Error during reconciliation for playground ID {}: {}", playgroundId, e.getMessage());
        }
    }

    public void startIndividualReconciliation(UUID reconciliationId) {

        try {
            csvComparator.runReconciliation(reconciliationId.toString());
        } catch (SQLException e) {
            LOGGER.error("Error during reconciliation for ID {}: {}", reconciliationId, e.getMessage());
        }
    }
}
