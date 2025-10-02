package com.annihilator.data.playground.utility;

import com.annihilator.data.playground.cloud.aws.S3Service;
import com.annihilator.data.playground.db.ReconciliationMappingDAO;
import com.annihilator.data.playground.db.ReconciliationResultsDAO;
import com.annihilator.data.playground.db.TaskDAO;
import com.annihilator.data.playground.model.Reconciliation;
import com.annihilator.data.playground.model.ReconciliationResultResponse;
import com.annihilator.data.playground.model.Status;
import com.annihilator.data.playground.model.Task;
import com.annihilator.data.playground.notification.NotificationService;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Utility class for monitoring DAG execution and sending comprehensive email notifications.
 * Takes a DAG ID in constructor and monitors all tasks within that DAG.
 */
public class DAGNotificationUtility {

    private final UUID dagId;
    private final TaskDAO taskDAO;
    private final S3Service s3Service;
    private final NotificationService notificationService;
    private final ReconciliationMappingDAO reconciliationMappingDAO;
    private final ReconciliationResultsDAO reconciliationResultsDAO;

    public DAGNotificationUtility(UUID dagId, TaskDAO taskDAO, S3Service s3Service, 
                                 NotificationService notificationService,
                                 ReconciliationMappingDAO reconciliationMappingDAO,
                                 ReconciliationResultsDAO reconciliationResultsDAO) {
        this.dagId = dagId;
        this.taskDAO = taskDAO;
        this.s3Service = s3Service;
        this.notificationService = notificationService;
        this.reconciliationMappingDAO = reconciliationMappingDAO;
        this.reconciliationResultsDAO = reconciliationResultsDAO;
    }

    /**
     * Monitors all tasks in the DAG and sends comprehensive email notification.
     * 
     * @param emailSubject Subject for the email notification
     * @param emailRecipient Recipient email address
     * @throws NotificationException if email sending fails
     */
    public void sendDAGExecutionNotification(String emailSubject, String emailRecipient) throws NotificationException {
        try {
            // Get all tasks for this DAG
            List<Task> tasks = getTasksByDAGId();
            
            // Build comprehensive email content
            String emailContent = buildDAGExecutionEmail(tasks);
            
            // Send email notification
            notificationService.notify(emailSubject, emailContent, emailRecipient);
            
        } catch (Exception e) {
            throw new NotificationException("Failed to send DAG execution notification: " + e.getMessage(), e);
        }
    }

    /**
     * Gets all tasks associated with this DAG ID.
     */
    private List<Task> getTasksByDAGId() {
        try {
            return taskDAO.findTasksByPlaygroundRecursively(dagId);
        } catch (Exception e) {
            throw new RuntimeException("Failed to retrieve tasks for DAG: " + dagId, e);
        }
    }

    /**
     * Builds comprehensive email content with task statuses and previews.
     */
    private String buildDAGExecutionEmail(List<Task> tasks) {
        StringBuilder emailContent = new StringBuilder();
        
        emailContent.append("<html>");
        emailContent.append("<head>");
        emailContent.append("<style>");
        emailContent.append("body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: 0; padding: 20px; background-color: #f5f5f5; }");
        emailContent.append(".container { max-width: 800px; margin: 0 auto; background-color: white; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }");
        emailContent.append(".header { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 20px; border-radius: 8px 8px 0 0; }");
        emailContent.append(".content { padding: 20px; }");
        emailContent.append(".summary { background-color: #f8f9fa; padding: 15px; border-radius: 6px; margin-bottom: 20px; }");
        emailContent.append(".summary-stats { display: inline-block; margin-right: 20px; }");
        emailContent.append("table { width: 100%; border-collapse: collapse; margin: 15px 0; font-size: 14px; }");
        emailContent.append("th { background-color: #343a40; color: white; padding: 12px 8px; text-align: left; font-weight: 600; }");
        emailContent.append("td { padding: 10px 8px; border-bottom: 1px solid #dee2e6; }");
        emailContent.append("tr:nth-child(even) { background-color: #f8f9fa; }");
        emailContent.append(".status-success { color: #155724; font-weight: bold; }");
        emailContent.append(".status-failed { color: #721c24; font-weight: bold; }");
        emailContent.append(".status-skipped { color: #856404; font-weight: bold; }");
        emailContent.append(".status-cancelled { color: #721c24; font-weight: bold; }");
        emailContent.append(".status-upstream-failed { color: #721c24; font-weight: bold; }");
        emailContent.append(".section-title { color: #495057; font-size: 18px; font-weight: 600; margin: 25px 0 15px 0; border-bottom: 2px solid #e9ecef; padding-bottom: 5px; }");
        emailContent.append(".reconciliation-item { background-color: #f8f9fa; padding: 15px; margin: 10px 0; border-radius: 6px; border-left: 4px solid #007bff; }");
        emailContent.append(".reconciliation-success { border-left-color: #28a745; }");
        emailContent.append(".reconciliation-failed { border-left-color: #dc3545; }");
        emailContent.append(".stats-grid { display: flex; justify-content: space-around; gap: 20px; margin: 10px 0; flex-wrap: wrap; }");
        emailContent.append(".stat-item { text-align: center; padding: 8px; background-color: white; border-radius: 4px; }");
        emailContent.append(".stat-number { font-size: 16px; font-weight: bold; color: #495057; }");
        emailContent.append(".stat-label { font-size: 12px; color: #6c757d; }");
        emailContent.append("</style>");
        emailContent.append("</head>");
        emailContent.append("<body>");
        
        emailContent.append("<div class='container'>");
        emailContent.append("<div class='header'>");
        emailContent.append("<h2 style='margin: 0; font-size: 24px;'>DAG Execution Report</h2>");
        emailContent.append("<p style='margin: 5px 0 0 0; opacity: 0.9;'>DAG ID: ").append(dagId).append("</p>");
        emailContent.append("</div>");
        
        emailContent.append("<div class='content'>");
        
        // Summary statistics
        long successCount = tasks.stream().filter(t -> Status.SUCCESS.equals(t.getLastRunStatus())).count();
        long failedCount = tasks.stream().filter(t -> Status.FAILED.equals(t.getLastRunStatus())).count();
        long cancelledCount = tasks.stream().filter(t -> Status.CANCELLED.equals(t.getLastRunStatus())).count();
        long skippedCount = tasks.stream().filter(t -> Status.SKIPPED.equals(t.getLastRunStatus())).count();
        long upstreamFailedCount = tasks.stream().filter(t -> Status.UPSTREAM_FAILED.equals(t.getLastRunStatus())).count();
        
        emailContent.append("<div class='summary'>");
        emailContent.append("<div class='stats-grid'>");
        emailContent.append("<div class='stat-item'><div class='stat-number'>").append(tasks.size()).append("</div><div class='stat-label'>Total Tasks</div></div>");
        emailContent.append("<div class='stat-item'><div class='stat-number' style='color: #28a745;'>").append(successCount).append("</div><div class='stat-label'>Success</div></div>");
        emailContent.append("<div class='stat-item'><div class='stat-number' style='color: #dc3545;'>").append(failedCount).append("</div><div class='stat-label'>Failed</div></div>");
        emailContent.append("<div class='stat-item'><div class='stat-number' style='color: #ffc107;'>").append(skippedCount).append("</div><div class='stat-label'>Skipped</div></div>");
        if (cancelledCount > 0) {
            emailContent.append("<div class='stat-item'><div class='stat-number' style='color: #fd7e14;'>").append(cancelledCount).append("</div><div class='stat-label'>Cancelled</div></div>");
        }
        if (upstreamFailedCount > 0) {
            emailContent.append("<div class='stat-item'><div class='stat-number' style='color: #dc3545;'>").append(upstreamFailedCount).append("</div><div class='stat-label'>Upstream Failed</div></div>");
        }
        emailContent.append("</div>");
        emailContent.append("</div>");
        
        // Task execution table
        emailContent.append("<div class='section-title'>Task Execution Results</div>");
        emailContent.append("<table>");
        emailContent.append("<thead>");
        emailContent.append("<tr><th>Step Name</th><th>Status</th></tr>");
        emailContent.append("</thead>");
        emailContent.append("<tbody>");
        
        // Process each task
        for (Task task : tasks) {
            processTaskForEmailTable(task, emailContent);
        }
        
        emailContent.append("</tbody>");
        emailContent.append("</table>");
        
        // Add reconciliation section
        addReconciliationSection(emailContent);
        
        emailContent.append("</div>"); // content
        emailContent.append("</div>"); // container
        emailContent.append("</body>");
        emailContent.append("</html>");
        
        return emailContent.toString();
    }

    /**
     * Processes individual task and adds its information to the email table.
     */
    private void processTaskForEmailTable(Task task, StringBuilder emailContent) {
        String statusClass = getStatusCssClass(task.getLastRunStatus());
        
        emailContent.append("<tr>");
        emailContent.append("<td>").append(escapeHtml(task.getName())).append("</td>");
        emailContent.append("<td><span class='").append(statusClass).append("'>").append(task.getLastRunStatus()).append("</span></td>");
        emailContent.append("</tr>");
    }

    /**
     * Gets CSS class name for status styling.
     */
    private String getStatusCssClass(Status status) {
        if (Status.SUCCESS.equals(status)) {
            return "status-success";
        } else if (Status.FAILED.equals(status) || Status.UPSTREAM_FAILED.equals(status)) {
            return "status-failed";
        } else if (Status.CANCELLED.equals(status)) {
            return "status-cancelled";
        } else if (Status.SKIPPED.equals(status)) {
            return "status-skipped";
        } else {
            return "status-skipped";
        }
    }

    /**
     * Adds reconciliation section to the email content.
     */
    private void addReconciliationSection(StringBuilder emailContent) {
        try {
            List<Reconciliation> reconciliations = reconciliationMappingDAO.findReconciliationMappingByPlaygroundId(dagId.toString());
            
            if (reconciliations.isEmpty()) {
                return; // Don't show reconciliation section if no reconciliations
            }
            
            emailContent.append("<div class='section-title'>Reconciliation Results</div>");
            emailContent.append("<table>");
            emailContent.append("<thead>");
            emailContent.append("<tr>");
            emailContent.append("<th>Left Task → Right Task</th>");
            emailContent.append("<th>Status</th>");
            emailContent.append("<th>Common</th>");
            emailContent.append("<th>Left Only</th>");
            emailContent.append("<th>Right Only</th>");
            emailContent.append("<th>Method</th>");
            emailContent.append("</tr>");
            emailContent.append("</thead>");
            emailContent.append("<tbody>");
            
            for (Reconciliation reconciliation : reconciliations) {
                processReconciliationForEmailTable(reconciliation, emailContent);
            }
            
            emailContent.append("</tbody>");
            emailContent.append("</table>");
            
        } catch (Exception e) {
            emailContent.append("<div class='section-title'>Reconciliation Results</div>");
            emailContent.append("<div class='reconciliation-item reconciliation-failed'>");
            emailContent.append("<strong>Error:</strong> Failed to retrieve reconciliation data - ").append(escapeHtml(e.getMessage()));
            emailContent.append("</div>");
        }
    }

    /**
     * Processes individual reconciliation and adds its information to the email table.
     */
    private void processReconciliationForEmailTable(Reconciliation reconciliation, StringBuilder emailContent) {
        try {
            ReconciliationResultResponse result = reconciliationResultsDAO.getReconciliationResult(reconciliation.getReconciliationId().toString());
            
            if (result == null) {
                emailContent.append("<tr>");
                emailContent.append("<td>").append(reconciliation.getReconciliationId()).append("</td>");
                emailContent.append("<td><span class='status-failed'>NO_RESULTS</span></td>");
                emailContent.append("<td>-</td><td>-</td><td>-</td><td>-</td>");
                emailContent.append("</tr>");
                return;
            }
            
            // Get task names using leftTableId and rightTableId
            String leftStepName = getTaskNameById(reconciliation.getLeftTableId());
            String rightStepName = getTaskNameById(reconciliation.getRightTableId());
            
            String statusClass = "SUCCESS".equals(result.getStatus()) ? "status-success" : "status-failed";
            
            emailContent.append("<tr>");
            emailContent.append("<td>").append(escapeHtml(leftStepName)).append(" → ").append(escapeHtml(rightStepName)).append("</td>");
            emailContent.append("<td><span class='").append(statusClass).append("'>").append(result.getStatus()).append("</span></td>");
            emailContent.append("<td>").append(formatNumber(result.getCommonRowCount())).append("</td>");
            emailContent.append("<td>").append(formatNumber(result.getLeftFileExclusiveRowCount())).append("</td>");
            emailContent.append("<td>").append(formatNumber(result.getRightFileExclusiveRowCount())).append("</td>");
            emailContent.append("<td>").append(result.getReconciliationMethod()).append("</td>");
            emailContent.append("</tr>");
            
        } catch (Exception e) {
            emailContent.append("<tr>");
            emailContent.append("<td>").append(reconciliation.getReconciliationId()).append("</td>");
            emailContent.append("<td><span class='status-failed'>ERROR</span></td>");
            emailContent.append("<td colspan='4'>").append(escapeHtml(e.getMessage())).append("</td>");
            emailContent.append("</tr>");
        }
    }

    /**
     * Formats numbers with commas for better readability.
     */
    private String formatNumber(long number) {
        return String.format("%,d", number);
    }

    /**
     * Gets task name by task ID.
     */
    private String getTaskNameById(String taskId) {
        try {
            if (taskId == null || taskId.trim().isEmpty()) {
                return "Unknown Task";
            }
            
            Task task = taskDAO.findTaskById(taskId);
            return task != null ? task.getName() : "Task Not Found";
            
        } catch (Exception e) {
            return "Error: " + e.getMessage();
        }
    }

    /**
     * Gets CSS class name based on task status.
     */
    private String getStatusClass(Status status) {
        if (Status.SUCCESS.equals(status)) {
            return "success";
        } else if (Status.FAILED.equals(status) || Status.UPSTREAM_FAILED.equals(status)) {
            return "failed";
        } else if (Status.CANCELLED.equals(status)) {
            return "cancelled";
        } else if (Status.SKIPPED.equals(status)) {
            return "skipped";
        } else {
            return "running";
        }
    }

    /**
     * Escapes HTML special characters.
     */
    private String escapeHtml(String text) {
        if (text == null) return "";
        return text.replace("&", "&amp;")
                   .replace("<", "&lt;")
                   .replace(">", "&gt;")
                   .replace("\"", "&quot;")
                   .replace("'", "&#39;");
    }
}
