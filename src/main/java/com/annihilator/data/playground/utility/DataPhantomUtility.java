package com.annihilator.data.playground.utility;

import java.time.LocalTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

public class DataPhantomUtility {

    public static int parseTimeComponent(String component) {
        if (component.equals("*")) return 0; // Default to 0 for wildcard

        if (component.startsWith("*/")) {
            try {
                return Integer.parseInt(component.substring(2));
            } catch (NumberFormatException e) {
                return 0;
            }
        }

        try {
            return Integer.parseInt(component);
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    public static long getExecutionTimeInMillis(String cronExpression) {

        try {
            String[] parts = cronExpression.trim().split("\\s+");

            // Handle 5-part vs 6-part cron
            int offset = parts.length == 6 ? 1 : 0;

            // Parse minutes and hours
            int minutes = parseTimeComponent(parts[offset]); // minutes
            int hours = parseTimeComponent(parts[offset + 1]); // hours

            // Convert to milliseconds from midnight
            long totalMillis = (hours * 60 * 60 * 1000L) + (minutes * 60 * 1000L);

            return totalMillis;

        } catch (Exception e) {
            return -1;
        }
    }

    public static long getCurrentTimeInMillis() {

        LocalTime now = LocalTime.now(ZoneId.of("UTC"));
        int hour = now.getHour();
        int minute = now.getMinute();
        int second = now.getSecond();

        long timeInMillis = (hour * 60 * 60 * 1000L) + (minute * 60 * 1000L) + (second * 1000L);

        return timeInMillis;
    }

    /**
     * Calculates the next execution time in milliseconds from midnight for interval-based cron expressions.
     * This method handles cron expressions with intervals and returns the next occurrence based on the current time.
     * 
     * @param cronExpression The cron expression to parse
     * @return The next execution time in milliseconds from midnight, or Long.MAX_VALUE for invalid expressions, or -1 if no execution today
     */
    public static long getNextExecutionTimeInMillis(String cronExpression) {
        
        if (cronExpression == null || cronExpression.trim().isEmpty()) {
            return -1;
        }

        try {
            String[] parts = cronExpression.trim().split("\\s+");
            
            int offset = parts.length == 6 ? 1 : 0;
            
            String minutesPart = parts[offset];
            String hoursPart = parts[offset + 1];
            
            long currentTime = getCurrentTimeInMillis();
            
            if (minutesPart.startsWith("*/") && hoursPart.startsWith("*/")) {
                int minuteInterval = parseTimeComponent(minutesPart);
                int hourInterval = parseTimeComponent(hoursPart);
                
                if (minuteInterval == 0 || hourInterval == 0) {
                    return -1;
                }
                
                int totalMinuteInterval = hourInterval * 60;
                int intervalMinutes = Math.min(minuteInterval, totalMinuteInterval);
                long intervalMillis = intervalMinutes * 60 * 1000L;
                
                long intervalsPassed = currentTime / intervalMillis;
                long nextExecution = (intervalsPassed + 1) * intervalMillis;
                
                if (nextExecution >= 24 * 60 * 60 * 1000L) {
                    return -1;
                }
                
                return nextExecution;
                
            } else if (hoursPart.startsWith("*/")) {
                int hourInterval = parseTimeComponent(hoursPart);
                
                if (hourInterval == 0) {
                    return -1;
                }
                
                int minutes = parseTimeComponent(minutesPart);
                
                for (int hour = 0; hour < 24; hour += hourInterval) {
                    long executionTime = (hour * 60 * 60 * 1000L) + (minutes * 60 * 1000L);
                    
                    if (executionTime > currentTime) {
                        return executionTime;
                    }
                }
                
                return -1;
                
            } else if (minutesPart.startsWith("*/")) {
                int minuteInterval = parseTimeComponent(minutesPart);
                
                if (minuteInterval == 0) {
                    return -1;
                }
                
                int hours = parseTimeComponent(hoursPart);
                
                for (int hour = hours; hour < 24; hour++) {
                    for (int minute = 0; minute < 60; minute += minuteInterval) {
                        long executionTime = (hour * 60 * 60 * 1000L) + (minute * 60 * 1000L);
                        
                        if (executionTime > currentTime) {
                            return executionTime;
                        }
                    }
                }
                
                return -1;
                
            } else {
                long result = getExecutionTimeInMillis(cronExpression);

                return result;
            }
            
        } catch (Exception e) {
            return -1;
        }
    }

    /**
     * Formats CSV preview data for better presentation.
     * Converts raw CSV lines into a formatted table structure.
     * 
     * @param csvLines List of CSV lines (first line should be headers)
     * @return Formatted table data with headers and rows
     */
    public static FormattedTable formatTable(List<String> csvLines) {
        if (csvLines == null || csvLines.isEmpty()) {
            return new FormattedTable(new ArrayList<>(), new ArrayList<>());
        }

        List<String> headers = new ArrayList<>();
        List<List<String>> rows = new ArrayList<>();

        // Parse headers (first line)
        if (!csvLines.isEmpty()) {
            headers.addAll(parseCsvLine(csvLines.get(0)));
        }

        // Parse data rows
        for (int i = 1; i < csvLines.size(); i++) {
            List<String> row = parseCsvLine(csvLines.get(i));
            rows.add(row);
        }

        return new FormattedTable(headers, rows);
    }

    /**
     * Parses a single CSV line into individual fields.
     * Handles basic CSV parsing with comma separators.
     * 
     * @param csvLine The CSV line to parse
     * @return List of parsed fields
     */
    private static List<String> parseCsvLine(String csvLine) {
        List<String> fields = new ArrayList<>();
        if (csvLine == null || csvLine.trim().isEmpty()) {
            return fields;
        }

        StringBuilder currentField = new StringBuilder();
        boolean inQuotes = false;
        
        for (int i = 0; i < csvLine.length(); i++) {
            char c = csvLine.charAt(i);
            
            if (c == '"') {
                inQuotes = !inQuotes;
            } else if (c == ',' && !inQuotes) {
                fields.add(currentField.toString().trim());
                currentField = new StringBuilder();
            } else {
                currentField.append(c);
            }
        }
        
        fields.add(currentField.toString().trim());
        return fields;
    }

    /**
     * Formats CSV data as HTML table for email notifications.
     * 
     * @param csvLines List of CSV lines (first line should be headers)
     * @param message Main message content
     * @return Complete HTML email content with formatted table
     */
    public static String formatTableAsHtml(List<String> csvLines, String message) {
        FormattedTable table = formatTable(csvLines);
        
        StringBuilder html = new StringBuilder();
        
        html.append("<html>");
        html.append("<head>");
        html.append("<style>");
        html.append("table { border-collapse: collapse; width: 100%; margin: 20px 0; }");
        html.append("th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }");
        html.append("th { background-color: #f2f2f2; font-weight: bold; }");
        html.append("tr:nth-child(even) { background-color: #f9f9f9; }");
        html.append("body { font-family: Arial, sans-serif; margin: 20px; }");
        html.append("</style>");
        html.append("</head>");
        html.append("<body>");
        
        html.append("<h2>Data Phantom Task Notification</h2>");
        html.append("<p>").append(escapeHtml(message)).append("</p>");
        
        if (table.getRowCount() > 0) {
            html.append("<h3>Preview Data (").append(table.getRowCount()).append(" rows)</h3>");
            html.append("<table>");
            
            // Add headers
            html.append("<thead><tr>");
            for (String header : table.getHeaders()) {
                html.append("<th>").append(escapeHtml(header)).append("</th>");
            }
            html.append("</tr></thead>");
            
            // Add data rows
            html.append("<tbody>");
            for (List<String> row : table.getRows()) {
                html.append("<tr>");
                for (String cell : row) {
                    html.append("<td>").append(escapeHtml(cell)).append("</td>");
                }
                html.append("</tr>");
            }
            html.append("</tbody>");
            
            html.append("</table>");
        } else {
            html.append("<p><em>No preview data available.</em></p>");
        }
        
        html.append("</body>");
        html.append("</html>");
        
        return html.toString();
    }

    /**
     * Escapes HTML special characters.
     */
    private static String escapeHtml(String text) {
        if (text == null) return "";
        return text.replace("&", "&amp;")
                   .replace("<", "&lt;")
                   .replace(">", "&gt;")
                   .replace("\"", "&quot;")
                   .replace("'", "&#39;");
    }

    /**
     * Represents a formatted table with headers and rows.
     */
    public static class FormattedTable {
        private final List<String> headers;
        private final List<List<String>> rows;

        public FormattedTable(List<String> headers, List<List<String>> rows) {
            this.headers = headers;
            this.rows = rows;
        }

        public List<String> getHeaders() {
            return headers;
        }

        public List<List<String>> getRows() {
            return rows;
        }

        public int getColumnCount() {
            return headers.size();
        }

        public int getRowCount() {
            return rows.size();
        }
    }

}
