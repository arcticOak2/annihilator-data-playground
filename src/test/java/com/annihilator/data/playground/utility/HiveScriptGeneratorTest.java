package com.annihilator.data.playground.utility;

import com.annihilator.data.playground.model.Task;
import com.annihilator.data.playground.model.UDF;
import com.annihilator.data.playground.db.UDFDAO;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.sql.SQLException;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Comprehensive tests for HiveScriptGenerator script generation.
 * These tests verify the actual generated scripts for different scenarios.
 */
class HiveScriptGeneratorTest {

    @Mock
    private UDFDAO udfDAO;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void testGenerateHiveScript_WithoutUDFs_ShouldGenerateBasicScript() throws SQLException {
        // Given
        Task task = new Task();
        task.setId(UUID.fromString("e8e9227d-d590-44bc-99c1-8ee43fee0f20"));
        task.setPlaygroundId(UUID.fromString("9b16e650-9201-4449-b532-3dd028fcf2bc"));
        task.setUdfIds(""); // No UDFs
        task.setQuery("SELECT product_name, sum(quantity) as total_quantity FROM sales_data GROUP BY product_name");

        // When
        List<String> script = HiveScriptGenerator.generateHiveScript(
            task, 
            udfDAO, 
            "9b16e650-9201-4449-b532-3dd028fcf2bc", 
            "e8e9227d-d590-44bc-99c1-8ee43fee0f20", 
            "abc123", 
            "test-bucket", 
            "2025-01-20", 
            "/tmp/output.csv", 
            "s3://test-bucket/output/result.csv"
        );

        // Then
        assertNotNull(script);
        assertFalse(script.isEmpty());

        // Print script for manual verification
        System.out.println("=== GENERATED HIVE SCRIPT WITHOUT UDFs ===");
        script.forEach(System.out::println);
        System.out.println("=== END SCRIPT ===\n");

        // Verify basic script structure
        assertTrue(script.stream().anyMatch(line -> line.contains("#!/bin/bash")));
        assertTrue(script.stream().anyMatch(line -> line.contains("# Hive Query Execution Script")));
        assertTrue(script.stream().anyMatch(line -> line.contains("set -e")));
        assertTrue(script.stream().anyMatch(line -> line.contains("LOG_FILE=\"/tmp/hive-execution-abc123.log\"")));
        
        // Verify query execution
        assertTrue(script.stream().anyMatch(line -> line.contains("echo \"Query:")));
        assertTrue(script.stream().anyMatch(line -> line.contains("hive -e \"SET hive.cli.print.header=true;")));
        
        // Verify the actual query is present
        assertTrue(script.stream().anyMatch(line -> line.contains("SELECT product_name, sum(quantity) as total_quantity FROM sales_data GROUP BY product_name")));
        
        // Verify S3 operations
        assertTrue(script.stream().anyMatch(line -> line.contains("aws s3 cp /tmp/output.csv s3://test-bucket/output/result.csv")));
        
        // Verify no UDF setup section
        assertFalse(script.stream().anyMatch(line -> line.contains("aws s3 cp s3://test-bucket/udfs/")));
        assertFalse(script.stream().anyMatch(line -> line.contains("CREATE TEMPORARY FUNCTION")));
    }

    @Test
    void testGenerateHiveScript_WithUDFs_ShouldGenerateCompleteScript() throws SQLException {
        // Given
        Task task = new Task();
        task.setId(UUID.fromString("e8e9227d-d590-44bc-99c1-8ee43fee0f20"));
        task.setPlaygroundId(UUID.fromString("9b16e650-9201-4449-b532-3dd028fcf2bc"));
        task.setUdfIds("udf1,udf2"); // Two UDFs
        task.setQuery("SELECT product_name, toUpperCase(product_name) as product_upper, toLowerCase(product_name) as product_lower, sum(quantity) as total_quantity FROM sales_data GROUP BY product_name");

        // Mock UDFs
        UDF udf1 = new UDF();
        udf1.setId("udf1");
        udf1.setName("String Uppercase");
        udf1.setFunctionName("toUpperCase");
        udf1.setJarS3Path("s3://test-bucket/udfs/string-utils.jar");
        udf1.setClassName("com.annihilator.dataphantom.udfs.StringUtils");

        UDF udf2 = new UDF();
        udf2.setId("udf2");
        udf2.setName("String Lowercase");
        udf2.setFunctionName("toLowerCase");
        udf2.setJarS3Path("s3://test-bucket/udfs/string-utils.jar");
        udf2.setClassName("com.annihilator.dataphantom.udfs.StringUtils");

        when(udfDAO.getUDFById("udf1")).thenReturn(udf1);
        when(udfDAO.getUDFById("udf2")).thenReturn(udf2);

        // When
        List<String> script = HiveScriptGenerator.generateHiveScript(
            task, 
            udfDAO, 
            "9b16e650-9201-4449-b532-3dd028fcf2bc", 
            "e8e9227d-d590-44bc-99c1-8ee43fee0f20", 
            "abc123", 
            "test-bucket", 
            "2025-01-20", 
            "/tmp/output.csv", 
            "s3://test-bucket/output/result.csv"
        );

        // Then
        assertNotNull(script);
        assertFalse(script.isEmpty());

        // Print script for manual verification
        System.out.println("=== GENERATED HIVE SCRIPT WITH UDFs ===");
        script.forEach(System.out::println);
        System.out.println("=== END SCRIPT ===\n");

        // Verify UDF setup exists
        assertTrue(script.stream().anyMatch(line -> line.contains("aws s3 cp s3://test-bucket/udfs/string-utils.jar /tmp/string-utils.jar")));
        assertTrue(script.stream().anyMatch(line -> line.contains("sudo cp /tmp/string-utils.jar /usr/lib/hive/lib/")));
        assertTrue(script.stream().anyMatch(line -> line.contains("CREATE TEMPORARY FUNCTION toUpperCase AS 'com.annihilator.dataphantom.udfs.StringUtils'")));
        assertTrue(script.stream().anyMatch(line -> line.contains("CREATE TEMPORARY FUNCTION toLowerCase AS 'com.annihilator.dataphantom.udfs.StringUtils'")));
        
        // Verify query execution
        assertTrue(script.stream().anyMatch(line -> line.contains("echo \"Query:")));
        assertTrue(script.stream().anyMatch(line -> line.contains("hive -e \"SET hive.cli.print.header=true;")));
        
        // Verify the actual query is present
        assertTrue(script.stream().anyMatch(line -> line.contains("SELECT product_name, toUpperCase(product_name) as product_upper, toLowerCase(product_name) as product_lower, sum(quantity) as total_quantity FROM sales_data GROUP BY product_name")));
        
        // Verify hive command (when UDFs are present)
        assertTrue(script.stream().anyMatch(line -> line.contains("hive -e \"SET hive.cli.print.header=true;")));
        
        // Verify S3 operations
        assertTrue(script.stream().anyMatch(line -> line.contains("aws s3 cp /tmp/output.csv s3://test-bucket/output/result.csv")));
        
        // Verify UDF DAO interactions
        verify(udfDAO).getUDFById("udf1");
        verify(udfDAO).getUDFById("udf2");
    }

    @Test
    void testGenerateHiveScript_WithMissingUDF_ShouldHandleGracefully() throws SQLException {
        // Given
        Task task = new Task();
        task.setId(UUID.fromString("e8e9227d-d590-44bc-99c1-8ee43fee0f20"));
        task.setPlaygroundId(UUID.fromString("9b16e650-9201-4449-b532-3dd028fcf2bc"));
        task.setUdfIds("valid-udf-id,nonexistent-udf-id"); // One valid, one missing
        task.setQuery("SELECT campaign_name, toUpperCase(campaign_name) as campaign_upper FROM campaign_data GROUP BY campaign_name");

        // Mock valid UDF
        UDF validUdf = new UDF();
        validUdf.setId("valid-udf-id");
        validUdf.setName("String Uppercase");
        validUdf.setFunctionName("toUpperCase");
        validUdf.setJarS3Path("s3://test-bucket/udfs/string-utils.jar");
        validUdf.setClassName("com.annihilator.dataphantom.udfs.StringUtils");

        when(udfDAO.getUDFById("valid-udf-id")).thenReturn(validUdf);
        when(udfDAO.getUDFById("nonexistent-udf-id")).thenReturn(null);

        // When
        List<String> script = HiveScriptGenerator.generateHiveScript(
            task, 
            udfDAO, 
            "9b16e650-9201-4449-b532-3dd028fcf2bc", 
            "e8e9227d-d590-44bc-99c1-8ee43fee0f20", 
            "abc123", 
            "test-bucket", 
            "2025-01-20", 
            "/tmp/output.csv", 
            "s3://test-bucket/output/result.csv"
        );

        // Then
        assertNotNull(script);
        assertFalse(script.isEmpty());

        // Print script for manual verification
        System.out.println("=== GENERATED HIVE SCRIPT WITH MISSING UDF ===");
        for (int i = 0; i < script.size(); i++) {
            System.out.printf("%3d: %s%n", i + 1, script.get(i));
        }
        System.out.println("=== END SCRIPT ===\n");

        // Verify valid UDF is processed
        assertTrue(script.stream().anyMatch(line -> line.contains("aws s3 cp s3://test-bucket/udfs/string-utils.jar /tmp/string-utils.jar")));
        assertTrue(script.stream().anyMatch(line -> line.contains("CREATE TEMPORARY FUNCTION toUpperCase AS")));
        
        // Verify missing UDF warning
        assertTrue(script.stream().anyMatch(line -> line.contains("Warning: UDF not found for ID: nonexistent-udf-id")));
        
        // Verify query execution still happens
        assertTrue(script.stream().anyMatch(line -> line.contains("echo \"Query:")));
        assertTrue(script.stream().anyMatch(line -> line.contains("SELECT campaign_name, toUpperCase(campaign_name) as campaign_upper FROM campaign_data GROUP BY campaign_name")));
        
        // Verify UDF DAO interactions
        verify(udfDAO).getUDFById("valid-udf-id");
        verify(udfDAO).getUDFById("nonexistent-udf-id");
    }

    @Test
    void testGenerateHiveScript_WithEmptyUdfIds_ShouldGenerateBasicScript() throws SQLException {
        // Given
        Task task = new Task();
        task.setId(UUID.fromString("e8e9227d-d590-44bc-99c1-8ee43fee0f20"));
        task.setPlaygroundId(UUID.fromString("9b16e650-9201-4449-b532-3dd028fcf2bc"));
        task.setUdfIds(null); // Null UDF IDs
        task.setQuery("SELECT product_name, sum(quantity) as total_quantity FROM sales_data GROUP BY product_name");

        // When
        List<String> script = HiveScriptGenerator.generateHiveScript(
            task, 
            udfDAO, 
            "9b16e650-9201-4449-b532-3dd028fcf2bc", 
            "e8e9227d-d590-44bc-99c1-8ee43fee0f20", 
            "abc123", 
            "test-bucket", 
            "2025-01-20", 
            "/tmp/output.csv", 
            "s3://test-bucket/output/result.csv"
        );

        // Then
        assertNotNull(script);
        assertFalse(script.isEmpty());

        // Verify no UDF setup section
        assertFalse(script.stream().anyMatch(line -> line.contains("aws s3 cp s3://test-bucket/udfs/")));
        assertFalse(script.stream().anyMatch(line -> line.contains("CREATE TEMPORARY FUNCTION")));
        
        // Verify basic script structure
        assertTrue(script.stream().anyMatch(line -> line.contains("#!/bin/bash")));
        assertTrue(script.stream().anyMatch(line -> line.contains("# Hive Query Execution Script")));
        assertTrue(script.stream().anyMatch(line -> line.contains("hive -e \"SET hive.cli.print.header=true;")));
        
        // Verify no UDF DAO interactions
        verifyNoInteractions(udfDAO);
    }

    @Test
    void testGenerateHiveScript_WithWhitespaceUdfIds_ShouldGenerateBasicScript() throws SQLException {
        // Given
        Task task = new Task();
        task.setId(UUID.fromString("e8e9227d-d590-44bc-99c1-8ee43fee0f20"));
        task.setPlaygroundId(UUID.fromString("9b16e650-9201-4449-b532-3dd028fcf2bc"));
        task.setUdfIds("   "); // Whitespace-only UDF IDs
        task.setQuery("SELECT product_name, sum(quantity) as total_quantity FROM sales_data GROUP BY product_name");

        // When
        List<String> script = HiveScriptGenerator.generateHiveScript(
            task, 
            udfDAO, 
            "9b16e650-9201-4449-b532-3dd028fcf2bc", 
            "e8e9227d-d590-44bc-99c1-8ee43fee0f20", 
            "abc123", 
            "test-bucket", 
            "2025-01-20", 
            "/tmp/output.csv", 
            "s3://test-bucket/output/result.csv"
        );

        // Then
        assertNotNull(script);
        assertFalse(script.isEmpty());

        // Verify no UDF setup section
        assertFalse(script.stream().anyMatch(line -> line.contains("aws s3 cp s3://test-bucket/udfs/")));
        assertFalse(script.stream().anyMatch(line -> line.contains("CREATE TEMPORARY FUNCTION")));
        
        // Verify basic script structure
        assertTrue(script.stream().anyMatch(line -> line.contains("#!/bin/bash")));
        assertTrue(script.stream().anyMatch(line -> line.contains("# Hive Query Execution Script")));
        assertTrue(script.stream().anyMatch(line -> line.contains("hive -e \"SET hive.cli.print.header=true;")));
        
        // Verify no UDF DAO interactions
        verifyNoInteractions(udfDAO);
    }

    @Test
    void testGenerateHiveScript_WithSQLException_ShouldPropagateException() throws SQLException {
        // Given
        Task task = new Task();
        task.setId(UUID.fromString("e8e9227d-d590-44bc-99c1-8ee43fee0f20"));
        task.setPlaygroundId(UUID.fromString("9b16e650-9201-4449-b532-3dd028fcf2bc"));
        task.setUdfIds("udf1");
        task.setQuery("SELECT campaign_name FROM campaign_data");

        when(udfDAO.getUDFById("udf1")).thenThrow(new SQLException("Database connection failed"));

        // When & Then
        assertThrows(SQLException.class, () -> {
            HiveScriptGenerator.generateHiveScript(
                task, 
                udfDAO, 
                "9b16e650-9201-4449-b532-3dd028fcf2bc", 
                "e8e9227d-d590-44bc-99c1-8ee43fee0f20", 
                "abc123", 
                "test-bucket", 
                "2025-01-20", 
                "/tmp/output.csv", 
                "s3://test-bucket/output/result.csv"
            );
        });

        verify(udfDAO).getUDFById("udf1");
    }
}
