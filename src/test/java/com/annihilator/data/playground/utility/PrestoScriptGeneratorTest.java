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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Comprehensive tests for UDFUtility script generation.
 * These tests verify the actual generated scripts for different scenarios.
 */
class PrestoScriptGeneratorTest {

    @Mock
    private UDFDAO udfDAO;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void testGeneratePrestoScript_WithoutUDFs_ShouldGenerateCleanScript() throws SQLException {
        // Given
        Task task = new Task();
        task.setUdfIds(""); // No UDFs
        task.setQuery("SELECT product_name, sum(quantity) as total_quantity FROM sales_data GROUP BY product_name");

        // When
        List<String> script = PrestoScriptGenerator.generatePrestoScript(
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
        assertFalse(script.isEmpty());
        
        // Print the generated script for verification
        System.out.println("=== GENERATED SCRIPT WITHOUT UDFs ===");
        System.out.println(String.join("\n", script));
        System.out.println("=== END SCRIPT ===\n");

        // Verify script structure
        assertTrue(script.contains("#!/bin/bash"));
        assertTrue(script.contains("# Presto Query Execution Script"));
        assertTrue(script.contains("set -e"));
        assertTrue(script.contains("LOG_FILE=\"/tmp/presto-execution-abc123.log\""));
        assertTrue(script.stream().anyMatch(line -> line.contains("LOG_S3_PATH=\"s3://test-bucket/data-phantom/logs/")));
        
        // Verify no UDF setup section
        assertFalse(script.stream().anyMatch(line -> line.contains("aws s3 cp s3://test-bucket/udfs/")));
        assertFalse(script.stream().anyMatch(line -> line.contains("CREATE TEMPORARY FUNCTION")));
        
        // Verify query execution
        assertTrue(script.stream().anyMatch(line -> line.contains("echo \"Query:")));
        assertTrue(script.stream().anyMatch(line -> line.contains("presto-cli --catalog hive")));
        
        // Verify the actual query is present
        assertTrue(script.stream().anyMatch(line -> line.contains("SELECT product_name, sum(quantity) as total_quantity FROM sales_data GROUP BY product_name")));
        
        // Verify presto-cli command
        assertTrue(script.stream().anyMatch(line -> line.contains("presto-cli --catalog hive --schema core --output-format CSV_HEADER")));
        
        // Verify S3 operations
        assertTrue(script.stream().anyMatch(line -> line.contains("aws s3 cp /tmp/output.csv s3://test-bucket/output/result.csv")));
        
        // Verify no UDF DAO interactions
        verifyNoInteractions(udfDAO);
    }

    @Test
    void testGeneratePrestoScript_WithUDFs_ShouldGenerateCompleteScript() throws SQLException {
        // Given
        Task task = new Task();
        task.setUdfIds("2a8e9ab9-3111-4892-8b55-9b13411bf4dc,3b9f0bc0-4222-5993-9c66-0ee44gff1fed"); // Two UDFs
        task.setQuery("SELECT product_name, toUpperCase(product_name) as product_upper, toLowerCase(product_name) as product_lower, sum(quantity) as total_quantity FROM sales_data GROUP BY product_name");

        // Mock UDF 1
        UDF udf1 = new UDF();
        udf1.setId("2a8e9ab9-3111-4892-8b55-9b13411bf4dc");
        udf1.setName("String Uppercase");
        udf1.setFunctionName("toUpperCase");
        udf1.setJarS3Path("s3://test-bucket/udfs/string-utils.jar");
        udf1.setClassName("com.annihilator.dataphantom.udfs.StringUtils");
        udf1.setParameterTypes("varchar");
        udf1.setReturnType("varchar");
        udf1.setDescription("Converts string to uppercase");

        // Mock UDF 2
        UDF udf2 = new UDF();
        udf2.setId("3b9f0bc0-4222-5993-9c66-0ee44gff1fed");
        udf2.setName("String Lowercase");
        udf2.setFunctionName("toLowerCase");
        udf2.setJarS3Path("s3://test-bucket/udfs/string-utils.jar");
        udf2.setClassName("com.annihilator.dataphantom.udfs.StringUtils");
        udf2.setParameterTypes("varchar");
        udf2.setReturnType("varchar");
        udf2.setDescription("Converts string to lowercase");

        when(udfDAO.getUDFById("2a8e9ab9-3111-4892-8b55-9b13411bf4dc")).thenReturn(udf1);
        when(udfDAO.getUDFById("3b9f0bc0-4222-5993-9c66-0ee44gff1fed")).thenReturn(udf2);

        // When
        List<String> script = PrestoScriptGenerator.generatePrestoScript(
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
        assertFalse(script.isEmpty());
        
        // Print the generated script for verification
        System.out.println("=== GENERATED SCRIPT WITH UDFs ===");
        System.out.println(String.join("\n", script));
        System.out.println("=== END SCRIPT ===\n");

        // Verify script structure
        assertTrue(script.contains("#!/bin/bash"));
        assertTrue(script.contains("# Presto Query Execution Script"));
        assertTrue(script.contains("set -e"));
        assertTrue(script.contains("LOG_FILE=\"/tmp/presto-execution-abc123.log\""));
        
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
        verify(udfDAO).getUDFById("2a8e9ab9-3111-4892-8b55-9b13411bf4dc");
        verify(udfDAO).getUDFById("3b9f0bc0-4222-5993-9c66-0ee44gff1fed");
    }

    @Test
    void testGeneratePrestoScript_WithMissingUDF_ShouldHandleGracefully() throws SQLException {
        // Given
        Task task = new Task();
        task.setUdfIds("2a8e9ab9-3111-4892-8b55-9b13411bf4dc,nonexistent-udf-id"); // One valid, one missing
        task.setQuery("SELECT campaign_name, toUpperCase(campaign_name) as campaign_upper FROM campaign_data GROUP BY campaign_name");

        // Mock only the first UDF
        UDF udf1 = new UDF();
        udf1.setId("2a8e9ab9-3111-4892-8b55-9b13411bf4dc");
        udf1.setName("String Uppercase");
        udf1.setFunctionName("toUpperCase");
        udf1.setJarS3Path("s3://test-bucket/udfs/string-utils.jar");
        udf1.setClassName("com.annihilator.dataphantom.udfs.StringUtils");
        udf1.setParameterTypes("varchar");
        udf1.setReturnType("varchar");

        when(udfDAO.getUDFById("2a8e9ab9-3111-4892-8b55-9b13411bf4dc")).thenReturn(udf1);
        when(udfDAO.getUDFById("nonexistent-udf-id")).thenReturn(null);

        // When
        List<String> script = PrestoScriptGenerator.generatePrestoScript(
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
        assertFalse(script.isEmpty());
        
        // Print the generated script for verification
        System.out.println("=== GENERATED SCRIPT WITH MISSING UDF ===");
        for (int i = 0; i < script.size(); i++) {
            System.out.println(String.format("%3d: %s", i + 1, script.get(i)));
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
        verify(udfDAO).getUDFById("2a8e9ab9-3111-4892-8b55-9b13411bf4dc");
        verify(udfDAO).getUDFById("nonexistent-udf-id");
    }
}
