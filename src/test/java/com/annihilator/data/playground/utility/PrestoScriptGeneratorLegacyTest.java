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
 * Unit tests for UDFUtility class.
 */
class PrestoScriptGeneratorLegacyTest {

    @Mock
    private UDFDAO udfDAO;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void testGeneratePrestoScript_WithNoUDFs_ShouldGenerateBasicScript() throws SQLException {
        // Given
        Task task = new Task();
        task.setUdfIds(""); // Empty UDF IDs
        task.setQuery("SELECT * FROM test_table");

        // When
        List<String> script = PrestoScriptGenerator.generatePrestoScript(task, udfDAO, "playground123", "query456", "unique789", "test-bucket", "2025-01-20", "/tmp/output.csv", "s3://bucket/output.csv");

        // Then
        assertFalse(script.isEmpty());
        assertTrue(script.contains("#!/bin/bash"));
        
        // Check that the query is in the script (either in echo or in presto-cli command)
        boolean queryFound = script.stream().anyMatch(line -> line.contains("SELECT * FROM test_table"));
        assertTrue(queryFound, "Query should be present in the script");
        
        // Check that the presto-cli command is in the script
        boolean prestoCliFound = script.stream().anyMatch(line -> line.contains("presto-cli --catalog hive --schema core --output-format CSV_HEADER"));
        assertTrue(prestoCliFound, "Presto CLI command should be present in the script");
        verifyNoInteractions(udfDAO);
    }

    @Test
    void testGeneratePrestoScript_WithNullUDFs_ShouldGenerateBasicScript() throws SQLException {
        // Given
        Task task = new Task();
        task.setUdfIds(null); // Null UDF IDs
        task.setQuery("SELECT * FROM test_table");

        // When
        List<String> script = PrestoScriptGenerator.generatePrestoScript(task, udfDAO, "playground123", "query456", "unique789", "test-bucket", "2025-01-20", "/tmp/output.csv", "s3://bucket/output.csv");

        // Then
        assertFalse(script.isEmpty());
        assertTrue(script.contains("#!/bin/bash"));
        
        // Check that the query is in the script
        boolean queryFound = script.stream().anyMatch(line -> line.contains("SELECT * FROM test_table"));
        assertTrue(queryFound, "Query should be present in the script");
        
        verifyNoInteractions(udfDAO);
    }

    @Test
    void testGeneratePrestoScript_WithSingleUDF_ShouldGenerateCompleteScript() throws SQLException {
        // Given
        String udfId = "2a8e9ab9-3111-4892-8b55-9b13411bf4dc";
        Task task = new Task();
        task.setUdfIds(udfId);
        task.setQuery("SELECT toUpperCase('hello') as result");

        UDF udf = new UDF();
        udf.setId(udfId);
        udf.setName("String uppercase");
        udf.setFunctionName("toUpperCase");
        udf.setJarS3Path("s3://bucket/udfs/test-udf.jar");
        udf.setClassName("com.annihilator.dataphantom.udfs.TestUDF");
        udf.setParameterTypes("varchar");
        udf.setReturnType("varchar");

        when(udfDAO.getUDFById(udfId)).thenReturn(udf);

        // When
        List<String> script = PrestoScriptGenerator.generatePrestoScript(task, udfDAO, "playground123", "query456", "unique789", "test-bucket", "2025-01-20", "/tmp/output.csv", "s3://bucket/output.csv");

        // Then
        assertFalse(script.isEmpty());
        assertTrue(script.contains("#!/bin/bash"));
        assertTrue(script.contains("aws s3 cp s3://bucket/udfs/test-udf.jar /tmp/test-udf.jar"));
        assertTrue(script.contains("sudo cp /tmp/test-udf.jar /usr/lib/hive/lib/"));
        assertTrue(script.contains("hive -e \"CREATE TEMPORARY FUNCTION toUpperCase AS 'com.annihilator.dataphantom.udfs.TestUDF'\""));
        assertTrue(script.stream().anyMatch(line -> line.contains("echo \"Query:")));
        
        // Check that the query is in the script
        boolean queryFound = script.stream().anyMatch(line -> line.contains("SELECT toUpperCase('hello') as result"));
        assertTrue(queryFound, "Query should be present in the script");

        verify(udfDAO).getUDFById(udfId);
    }

    @Test
    void testGeneratePrestoScript_WithMultipleUDFs_ShouldGenerateCompleteScript() throws SQLException {
        // Given
        String udfId1 = "2a8e9ab9-3111-4892-8b55-9b13411bf4dc";
        String udfId2 = "3b9f0bc0-4222-5993-9c66-0ee44gff1fed";
        Task task = new Task();
        task.setUdfIds(udfId1 + "," + udfId2);
        task.setQuery("SELECT toUpperCase('hello'), toLowerCase('WORLD')");

        UDF udf1 = new UDF();
        udf1.setId(udfId1);
        udf1.setName("String uppercase");
        udf1.setFunctionName("toUpperCase");
        udf1.setJarS3Path("s3://bucket/udfs/test-udf.jar");
        udf1.setClassName("com.annihilator.dataphantom.udfs.TestUDF");
        udf1.setParameterTypes("varchar");
        udf1.setReturnType("varchar");

        UDF udf2 = new UDF();
        udf2.setId(udfId2);
        udf2.setName("String lowercase");
        udf2.setFunctionName("toLowerCase");
        udf2.setJarS3Path("s3://bucket/udfs/test-udf.jar");
        udf2.setClassName("com.annihilator.dataphantom.udfs.TestUDF");
        udf2.setParameterTypes("varchar");
        udf2.setReturnType("varchar");

        when(udfDAO.getUDFById(udfId1)).thenReturn(udf1);
        when(udfDAO.getUDFById(udfId2)).thenReturn(udf2);

        // When
        List<String> script = PrestoScriptGenerator.generatePrestoScript(task, udfDAO, "playground123", "query456", "unique789", "test-bucket", "2025-01-20", "/tmp/output.csv", "s3://bucket/output.csv");

        // Then
        assertFalse(script.isEmpty());
        assertTrue(script.contains("#!/bin/bash"));
        
        // Check that both UDFs are processed
        long downloadCommands = script.stream()
                .filter(cmd -> cmd.startsWith("aws s3 cp") && cmd.contains("udfs/"))
                .count();
        assertEquals(2, downloadCommands);

        long registerCommands = script.stream()
                .filter(cmd -> cmd.contains("CREATE TEMPORARY FUNCTION"))
                .count();
        assertEquals(2, registerCommands);

        // Check that the query is in the script
        boolean queryFound = script.stream().anyMatch(line -> line.contains("SELECT toUpperCase('hello'), toLowerCase('WORLD')"));
        assertTrue(queryFound, "Query should be present in the script");

        verify(udfDAO).getUDFById(udfId1);
        verify(udfDAO).getUDFById(udfId2);
    }

    @Test
    void testGeneratePrestoScript_WithUDFNotFound_ShouldHandleGracefully() throws SQLException {
        // Given
        String udfId = "nonexistent-udf-id";
        Task task = new Task();
        task.setUdfIds(udfId);
        task.setQuery("SELECT * FROM test_table");

        when(udfDAO.getUDFById(udfId)).thenReturn(null);

        // When
        List<String> script = PrestoScriptGenerator.generatePrestoScript(task, udfDAO, "playground123", "query456", "unique789", "test-bucket", "2025-01-20", "/tmp/output.csv", "s3://bucket/output.csv");

        // Then
        assertFalse(script.isEmpty());
        assertTrue(script.contains("#!/bin/bash"));
        assertTrue(script.contains("echo \"Warning: UDF not found for ID: " + udfId + "\" | tee -a ${LOG_FILE}"));
        
        // Check that the query is in the script
        boolean queryFound = script.stream().anyMatch(line -> line.contains("SELECT * FROM test_table"));
        assertTrue(queryFound, "Query should be present in the script");

        verify(udfDAO).getUDFById(udfId);
    }

    @Test
    void testGeneratePrestoScript_WithSQLException_ShouldPropagateException() throws SQLException {
        // Given
        String udfId = "2a8e9ab9-3111-4892-8b55-9b13411bf4dc";
        Task task = new Task();
        task.setUdfIds(udfId);
        task.setQuery("SELECT * FROM test_table");

        when(udfDAO.getUDFById(udfId)).thenThrow(new SQLException("Database error"));

        // When & Then
        assertThrows(SQLException.class, () -> {
            PrestoScriptGenerator.generatePrestoScript(task, udfDAO, "playground123", "query456", "unique789", "test-bucket", "2025-01-20", "/tmp/output.csv", "s3://bucket/output.csv");
        });

        verify(udfDAO).getUDFById(udfId);
    }
}
