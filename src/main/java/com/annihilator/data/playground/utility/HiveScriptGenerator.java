package com.annihilator.data.playground.utility;

import com.annihilator.data.playground.db.UDFDAO;
import com.annihilator.data.playground.model.Task;
import com.annihilator.data.playground.model.UDF;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Script generator for Hive execution scripts with UDF support. This class handles the dynamic
 * generation of complete Hive execution scripts including UDF setup (if any) and the main query
 * execution.
 */
public class HiveScriptGenerator {

  /**
   * Generates a complete Hive execution script for a given task. This includes UDF setup (if any)
   * and the main query execution.
   *
   * @param task The task containing query and UDF IDs
   * @param udfDAO The UDF DAO for fetching UDF metadata
   * @param playgroundId The playground ID for logging
   * @param queryId The query ID for logging
   * @param uniqueId The unique execution ID
   * @param bucket The S3 bucket for output
   * @param date The execution date
   * @param tempFile The temporary file path for output
   * @param outputPath The S3 output path
   * @return Complete Hive execution script as List of strings
   * @throws SQLException if there's an error fetching UDF metadata
   */
  public static List<String> generateHiveScript(
      Task task,
      UDFDAO udfDAO,
      String playgroundId,
      String queryId,
      String uniqueId,
      String bucket,
      String date,
      String tempFile,
      String outputPath)
      throws SQLException {
    return generateHiveScript(
        task,
        udfDAO,
        playgroundId,
        queryId,
        uniqueId,
        bucket,
        "data-phantom",
        date,
        tempFile,
        outputPath);
  }

  public static List<String> generateHiveScript(
      Task task,
      UDFDAO udfDAO,
      String playgroundId,
      String queryId,
      String uniqueId,
      String bucket,
      String pathPrefix,
      String date,
      String tempFile,
      String outputPath)
      throws SQLException {
    List<String> script = new ArrayList<>();

    // Generate script header
    script.add("#!/bin/bash");
    script.add("# Hive Query Execution Script");
    script.add("set -e");
    script.add("LOG_FILE=\"/tmp/hive-execution-" + uniqueId + ".log\"");
    script.add(
        "LOG_S3_PATH=\"s3://"
            + bucket
            + "/"
            + pathPrefix
            + "/logs/"
            + date
            + "/hive-log-"
            + playgroundId
            + "-"
            + queryId
            + "-"
            + uniqueId
            + ".log\"");
    script.add("");

    // UDF Setup Section (if UDFs exist)
    if (task.getUdfIds() != null && !task.getUdfIds().trim().isEmpty()) {
      String[] udfIds = task.getUdfIds().split(",");

      for (String udfId : udfIds) {
        udfId = udfId.trim();
        if (udfId.isEmpty()) continue;

        UDF udf = getUDFById(udfId, udfDAO);
        if (udf == null) {
          script.add("echo \"Warning: UDF not found for ID: " + udfId + "\" | tee -a ${LOG_FILE}");
          continue;
        }

        String jarFileName = extractJarFileName(udf.getJarS3Path());
        String localJarPath = "/tmp/" + jarFileName;

        script.add("aws s3 cp " + udf.getJarS3Path() + " " + localJarPath);
        script.add("sudo cp " + localJarPath + " /usr/lib/hive/lib/");
        script.add(
            "hive -e \"CREATE TEMPORARY FUNCTION "
                + udf.getFunctionName()
                + " AS '"
                + udf.getClassName()
                + "'\"");
      }
    }

    // Query Execution Section
    script.add("echo \"Query: " + task.getQuery() + "\" | tee -a ${LOG_FILE}");
    script.add(
        "hive -e \"SET hive.cli.print.header=true; "
            + escapeForShell(task.getQuery())
            + "\" > "
            + tempFile);
    script.add("hive_exit_code=$?");

    script.add("if [ ! -f " + tempFile + " ] || [ ! -s " + tempFile + " ]; then");
    script.add("    hive_exit_code=1");
    script.add("fi");

    script.add("if [[ $hive_exit_code == 0 ]]; then");
    script.add("    aws s3 cp " + tempFile + " " + outputPath);
    script.add("    if [[ $? != 0 ]]; then");
    script.add("        hive_exit_code=1");
    script.add("    fi");
    script.add("fi");
    script.add("");
    script.add("aws s3 cp ${LOG_FILE} ${LOG_S3_PATH}");
    script.add("");
    script.add("if [[ $hive_exit_code != 0 ]]; then");
    script.add("    exit 1");
    script.add("else");
    script.add("    exit 0");
    script.add("fi");

    return script;
  }

  /**
   * Generates a Presto CREATE FUNCTION command for the given UDF.
   *
   * @param udf The UDF metadata
   * @param localJarPath The local path to the JAR file
   * @return The formatted Presto CREATE FUNCTION command
   */
  private static String generateHiveUDFCommands(UDF udf, String localJarPath) {
    StringBuilder commands = new StringBuilder();

    // First, add the JAR to Hive session
    commands.append("ADD JAR '");
    commands.append(localJarPath);
    commands.append("'; ");

    // Then, create the function
    commands.append("CREATE FUNCTION ");
    commands.append(udf.getFunctionName());
    commands.append(" AS '");
    commands.append(udf.getClassName());
    commands.append("'");

    return commands.toString();
  }

  private static String generateHiveCreateFunctionCommand(UDF udf, String localJarPath) {
    StringBuilder command = new StringBuilder();
    command.append("CREATE FUNCTION ");
    command.append(udf.getFunctionName());
    command.append(" AS '");
    command.append(udf.getClassName());
    command.append("' USING JAR '");
    command.append(localJarPath);
    command.append("'");

    return command.toString();
  }

  private static String generatePrestoCreateFunctionCommand(UDF udf, String localJarPath) {
    StringBuilder command = new StringBuilder();
    command.append("CREATE FUNCTION IF NOT EXISTS ");
    command.append(udf.getFunctionName());
    command.append("(");

    // Convert parameter types to proper Presto format
    String[] paramTypes = udf.getParameterTypes().split(",");
    for (int i = 0; i < paramTypes.length; i++) {
      if (i > 0) command.append(", ");
      command.append("x").append(i + 1).append(" ").append(paramTypes[i].trim());
    }

    command.append(") RETURNS ");
    command.append(udf.getReturnType());
    command.append(" LANGUAGE java EXTERNAL NAME '");
    command.append(udf.getClassName());
    command.append(".");
    command.append(udf.getFunctionName());
    command.append("'");

    return command.toString();
  }

  /**
   * Extracts the JAR file name from an S3 path.
   *
   * @param s3Path The S3 path (e.g., "s3://bucket/path/to/file.jar")
   * @return The file name (e.g., "file.jar")
   */
  private static String extractJarFileName(String s3Path) {
    if (s3Path == null || s3Path.isEmpty()) {
      return "unknown.jar";
    }

    String[] parts = s3Path.split("/");
    return parts[parts.length - 1];
  }

  /**
   * Helper method to get UDF by ID.
   *
   * @param udfId The UDF ID
   * @param udfDAO The UDF DAO
   * @return The UDF object or null if not found
   * @throws SQLException if there's an error fetching UDF metadata
   */
  private static UDF getUDFById(String udfId, UDFDAO udfDAO) throws SQLException {
    return udfDAO.getUDFById(udfId);
  }

  /**
   * Escapes a string for safe use in shell commands.
   *
   * @param input The string to escape
   * @return The escaped string
   */
  private static String escapeForShell(String input) {
    if (input == null) {
      return "";
    }
    // Escape double quotes and backslashes for shell
    return input.replace("\\", "\\\\").replace("\"", "\\\"");
  }
}
