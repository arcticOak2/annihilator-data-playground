package com.annihilator.data.playground.utility;

import java.util.*;
import java.util.regex.*;

/**
 * Extracts selected fields from SQL queries, handling complex expressions,
 * aliases, functions, CASE statements, window functions, and subqueries.
 */
public class SQLQueryFieldExtractor {

    private static final Set<String> SQL_KEYWORDS = Set.of(
            "SELECT", "FROM", "WHERE", "GROUP", "BY", "HAVING", "ORDER", "LIMIT",
            "JOIN", "INNER", "LEFT", "RIGHT", "OUTER", "ON", "AS", "DISTINCT",
            "COUNT", "SUM", "AVG", "MIN", "MAX", "CASE", "WHEN", "THEN", "ELSE", "END",
            "ROW_NUMBER", "RANK", "DENSE_RANK", "OVER", "PARTITION", "WINDOW"
    );

    /**
     * Extracts the selected fields from a SQL query
     *
     * @param sqlQuery The SQL query to parse
     * @return List of field names/aliases
     */
    public static List<String> extractSelectedFields(String sqlQuery) {
        if (sqlQuery == null || sqlQuery.trim().isEmpty()) {
            return Collections.emptyList();
        }

        String cleanedQuery = cleanQuery(sqlQuery);
        String selectClause = extractMainSelectClause(cleanedQuery);

        if (selectClause == null) {
            return Collections.emptyList();
        }

        return parseSelectFields(selectClause);
    }

    /**
     * Cleans up the SQL query by removing comments and normalizing whitespace
     */
    private static String cleanQuery(String query) {
        return query
                .replaceAll("--.*$", "") // Remove single-line comments
                .replaceAll("/\\*.*?\\*/", "") // Remove multi-line comments
                .replaceAll("\\s+", " ") // Normalize whitespace
                .trim();
    }

    /**
     * Extracts the main SELECT clause from the query
     */
    private static String extractMainSelectClause(String query) {
        int selectIndex = query.toUpperCase().indexOf("SELECT");
        if (selectIndex == -1) return null;

        int fromIndex = findMainFromClause(query, selectIndex);
        if (fromIndex == -1) return null;

        String selectPart = query.substring(selectIndex + 6, fromIndex).trim();

        // Remove DISTINCT if present
        if (selectPart.toUpperCase().startsWith("DISTINCT")) {
            selectPart = selectPart.substring(8).trim();
        }

        return selectPart;
    }

    /**
     * Finds the main FROM clause by tracking parentheses
     */
    private static int findMainFromClause(String query, int startIndex) {
        int parenCount = 0;
        int fromIndex = startIndex;

        while (fromIndex < query.length()) {
            char c = query.charAt(fromIndex);

            if (c == '(') {
                parenCount++;
            } else if (c == ')') {
                parenCount--;
            } else if (c == ' ' && parenCount == 0) {
                String remaining = query.substring(fromIndex).toUpperCase();
                if (remaining.startsWith(" FROM ")) {
                    return fromIndex;
                }
            }
            fromIndex++;
        }

        return -1;
    }

    /**
     * Parses individual fields from the SELECT clause
     */
    private static List<String> parseSelectFields(String selectClause) {
        List<String> fields = new ArrayList<>();
        List<String> fieldStrings = splitByComma(selectClause);

        for (String fieldStr : fieldStrings) {
            fieldStr = fieldStr.trim();
            if (!fieldStr.isEmpty()) {
                String fieldName = extractFieldName(fieldStr, fieldStrings);
                if (fieldName != null && !fieldName.isEmpty()) {
                    fields.add(fieldName);
                }
            }
        }

        return fields;
    }

    /**
     * Splits SELECT fields by comma, handling nested functions and strings
     */
    private static List<String> splitByComma(String selectClause) {
        List<String> fields = new ArrayList<>();
        StringBuilder currentField = new StringBuilder();
        int parenCount = 0;
        int bracketCount = 0;
        boolean inString = false;
        char stringChar = 0;

        for (int i = 0; i < selectClause.length(); i++) {
            char c = selectClause.charAt(i);

            if (!inString) {
                if (c == '\'' || c == '"') {
                    inString = true;
                    stringChar = c;
                } else if (c == '(') {
                    parenCount++;
                } else if (c == ')') {
                    parenCount--;
                } else if (c == '[') {
                    bracketCount++;
                } else if (c == ']') {
                    bracketCount--;
                } else if (c == ',' && parenCount == 0 && bracketCount == 0) {
                    fields.add(currentField.toString());
                    currentField = new StringBuilder();
                    continue;
                }
            } else if (c == stringChar) {
                inString = false;
            }

            currentField.append(c);
        }

        if (currentField.length() > 0) {
            fields.add(currentField.toString());
        }

        return fields;
    }

    /**
     * Extracts the field name from a field expression
     */
    private static String extractFieldName(String fieldStr, List<String> allFields) {
        fieldStr = fieldStr.trim();

        // Handle SELECT * - return null to exclude from results
        if (fieldStr.equals("*")) {
            return null;
        }

        // Handle aliases (AS keyword)
        String alias = extractAlias(fieldStr);
        if (alias != null) {
            return alias;
        }

        // Handle CASE statements
        if (fieldStr.toUpperCase().startsWith("CASE")) {
            return extractCaseAlias(fieldStr);
        }

        // Handle window functions
        if (fieldStr.toUpperCase().contains("OVER(")) {
            return extractWindowFunctionAlias(fieldStr);
        }

        // Handle subqueries
        if (fieldStr.contains("(SELECT")) {
            return extractSubqueryAlias(fieldStr);
        }

        // Handle simple functions
        if (fieldStr.contains("(") && fieldStr.contains(")")) {
            return extractFunctionAlias(fieldStr);
        }

        // Handle simple column references
        if (fieldStr.contains(".") && !fieldStr.contains("(")) {
            String[] parts = fieldStr.split("\\.");
            if (parts.length == 2) {
                String tableAlias = parts[0].trim();
                String columnName = parts[1].trim();
                
                // Check if removing the prefix would cause a conflict
                if (isSafeToRemovePrefix(tableAlias, columnName, allFields)) {
                    return columnName; // Return just the column name
                } else {
                    return fieldStr; // Keep the full table.column name
                }
            }
        }
        
        return fieldStr;
    }

    /**
     * Checks if it's safe to remove table prefix from a column name
     * Returns true if removing the prefix won't cause naming conflicts
     */
    private static boolean isSafeToRemovePrefix(String tableAlias, String columnName, List<String> allFields) {
        // Count how many different tables have this column name
        Set<String> tablesWithThisColumn = new HashSet<>();
        
        for (String field : allFields) {
            if (field.contains(".") && !field.contains("(")) {
                String[] parts = field.split("\\.");
                if (parts.length == 2) {
                    String otherTableAlias = parts[0].trim();
                    String otherColumnName = parts[1].trim();
                    
                    if (otherColumnName.equals(columnName)) {
                        tablesWithThisColumn.add(otherTableAlias);
                    }
                }
            }
        }
        
        // Safe to remove prefix if only one table has this column name
        return tablesWithThisColumn.size() <= 1;
    }

    /**
     * Extracts alias from field expression
     */
    private static String extractAlias(String fieldStr) {
        // Look for AS alias - need to find the last AS that's not inside parentheses
        int parenCount = 0;
        int lastAsIndex = -1;
        
        for (int i = 0; i < fieldStr.length() - 1; i++) {
            char c = fieldStr.charAt(i);
            if (c == '(') {
                parenCount++;
            } else if (c == ')') {
                parenCount--;
            } else if (c == ' ' && parenCount == 0) {
                String remaining = fieldStr.substring(i).toUpperCase();
                if (remaining.startsWith(" AS ")) {
                    lastAsIndex = i;
                }
            }
        }
        
        if (lastAsIndex != -1) {
            String alias = fieldStr.substring(lastAsIndex + 4).trim();
            return alias;
        }

        // Look for space-separated alias (last word if no function)
        if (!fieldStr.contains("(")) {
            String[] parts = fieldStr.split("\\s+");
            if (parts.length > 1) {
                String lastPart = parts[parts.length - 1];
                if (!isSQLKeyword(lastPart)) {
                    return lastPart;
                }
            }
        }

        return null;
    }

    /**
     * Extracts alias from CASE statement
     */
    private static String extractCaseAlias(String fieldStr) {
        String alias = extractAlias(fieldStr);
        if (alias != null) {
            return alias;
        }
        
        // Try to extract meaningful name from CASE statement
        Pattern columnPattern = Pattern.compile("\\b(\\w+)\\.(\\w+)\\b");
        Matcher matcher = columnPattern.matcher(fieldStr);
        if (matcher.find()) {
            return matcher.group(2); // Return the column name
        }
        
        String[] words = fieldStr.split("\\s+");
        for (String word : words) {
            word = word.trim();
            if (!word.isEmpty() && !isSQLKeyword(word) && !word.equals("CASE") && !word.equals("WHEN") && 
                !word.equals("THEN") && !word.equals("ELSE") && !word.equals("END") && 
                !word.equals("IS") && !word.equals("NULL")) {
                return word;
            }
        }
        
        return "case_result";
    }

    /**
     * Extracts alias from window function
     */
    private static String extractWindowFunctionAlias(String fieldStr) {
        String alias = extractAlias(fieldStr);
        if (alias != null) {
            return alias;
        }

        Pattern funcPattern = Pattern.compile("(\\w+)\\s*\\(", Pattern.CASE_INSENSITIVE);
        Matcher matcher = funcPattern.matcher(fieldStr);
        if (matcher.find()) {
            return matcher.group(1).toLowerCase() + "_over";
        }

        return "window_function";
    }

    /**
     * Extracts alias from subquery
     */
    private static String extractSubqueryAlias(String fieldStr) {
        String alias = extractAlias(fieldStr);
        if (alias != null) {
            return alias;
        }
        return "subquery_result";
    }

    /**
     * Extracts alias from function
     */
    private static String extractFunctionAlias(String fieldStr) {
        String alias = extractAlias(fieldStr);
        if (alias != null) {
            return alias;
        }

        // Handle CAST functions specially - let the alias extraction handle it
        if (fieldStr.toUpperCase().startsWith("CAST")) {
            // The alias extraction should handle CAST functions correctly
            // Just return a generic name if no alias found
            return "cast_result";
        }

        // Handle nested parentheses by finding the matching closing parenthesis
        int openParen = fieldStr.indexOf('(');
        if (openParen != -1) {
            int parenCount = 0;
            int closeParen = -1;
            for (int i = openParen; i < fieldStr.length(); i++) {
                if (fieldStr.charAt(i) == '(') {
                    parenCount++;
                } else if (fieldStr.charAt(i) == ')') {
                    parenCount--;
                    if (parenCount == 0) {
                        closeParen = i;
                        break;
                    }
                }
            }
            
            if (closeParen != -1) {
                String funcName = fieldStr.substring(0, openParen).trim().toLowerCase();
                String innerContent = fieldStr.substring(openParen + 1, closeParen).trim();
                
                // For coalesce functions, return the full function as field name
                if ("coalesce".equals(funcName)) {
                    return funcName + "(" + innerContent + ")";
                }
                
                // For aggregate functions (SUM, AVG, COUNT, MIN, MAX), return the full function as field name
                if (Set.of("sum", "avg", "count", "min", "max").contains(funcName)) {
                    return funcName.toUpperCase() + "(" + innerContent + ")";
                }
                
                return funcName + "_" + innerContent;
            }
        }

        return "function_result";
    }

    /**
     * Checks if a word is a SQL keyword
     */
    private static boolean isSQLKeyword(String word) {
        return SQL_KEYWORDS.contains(word.toUpperCase());
    }
}