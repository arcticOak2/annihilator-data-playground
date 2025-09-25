package com.annihilator.data.playground.reconsilation;

import com.annihilator.data.playground.cloud.aws.S3Service;
import com.annihilator.data.playground.config.ReconciliationConfig;
import com.annihilator.data.playground.db.ReconciliationMappingDAO;
import com.annihilator.data.playground.db.ReconciliationResultsDAO;
import com.annihilator.data.playground.db.TaskDAO;
import com.annihilator.data.playground.model.CSVComparisonResult;
import com.annihilator.data.playground.model.Reconciliation;
import com.annihilator.data.playground.model.Task;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.opencsv.CSVReader;
import com.sangupta.bloomfilter.impl.InMemoryBloomFilter;
import org.slf4j.Logger;

import java.io.StringReader;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class AdaptiveCSVComparator implements CSVComparator {

    private static final Logger LOGGER = org.slf4j.LoggerFactory.getLogger(AdaptiveCSVComparator.class);
    private static final Gson GSON = new Gson();

    private S3Service s3Service;
    private ReconciliationMappingDAO reconciliationMappingDAO;
    private ReconciliationResultsDAO reconciliationResultsDAO;
    private ReconciliationConfig reconciliationConfig;
    private TaskDAO taskDAO;

    public AdaptiveCSVComparator(S3Service s3Service, ReconciliationMappingDAO reconciliationMappingDAO,
                                 ReconciliationResultsDAO reconciliationResultsDAO, TaskDAO taskDAO, ReconciliationConfig reconciliationConfig) {
        this.s3Service = s3Service;
        this.reconciliationMappingDAO = reconciliationMappingDAO;
        this.reconciliationResultsDAO = reconciliationResultsDAO;
        this.reconciliationConfig = reconciliationConfig;
        this.taskDAO = taskDAO;
    }

    @Override
    public void runAllReconciliation(String playgroundId) throws SQLException {
        List<Reconciliation> reconciliation = reconciliationMappingDAO.findReconciliationMappingByPlaygroundId(playgroundId);

        if (reconciliation.isEmpty()) {
            LOGGER.warn("No reconciliation mapping found for playground ID: {}", playgroundId);
            return;
        }

        for (Reconciliation rec : reconciliation) {
            processSingleReconciliation(rec);
        }
    }

    @Override
    public void runReconciliation(String playgroundId) throws SQLException {
        Reconciliation reconciliation = reconciliationMappingDAO.findReconciliationMappingById(playgroundId);

        if (Objects.isNull(reconciliation)) {
            LOGGER.warn("No reconciliation mapping found for ID: {}", playgroundId);
            return;
        }

        processSingleReconciliation(reconciliation);
    }

    private void processSingleReconciliation(Reconciliation reconciliation) throws SQLException {
        String reconciliationId = reconciliation.getReconciliationId().toString();
        Task leftTask = taskDAO.findTaskById(reconciliation.getLeftTableId());
        Task rightTask = taskDAO.findTaskById(reconciliation.getRightTableId());

        if (leftTask == null ||
                rightTask == null ||
                leftTask.getOutputLocation() == null ||
                rightTask.getOutputLocation() == null
        ) {
            LOGGER.error("Tasks not ready for reconciliation: {}", reconciliationId);
            storeFailedResult(reconciliationId);
            return;
        }

        Map<String, String> columnsMapping = transformToMap(reconciliation.getMapping());
        if (columnsMapping == null || columnsMapping.isEmpty()) {
            LOGGER.error("Invalid or empty mapping for reconciliation: {}", reconciliationId);
            storeFailedResult(reconciliationId);
            return;
        }

        long leftFileSize = s3Service.getS3FileSize(leftTask.getOutputLocation());
        long rightFileSize = s3Service.getS3FileSize(rightTask.getOutputLocation());
        
        if (leftFileSize == -1 || rightFileSize == -1) {
            LOGGER.error("Could not determine file sizes for reconciliation: {}", reconciliationId);
            storeFailedResult(reconciliationId);
            return;
        }

        long totalFileSize = leftFileSize + rightFileSize;
        boolean useBloomFilter = totalFileSize > reconciliationConfig.getExactMatchThreshold();

        LOGGER.info("Reconciliation {} - Total file size: {} bytes, Using Bloom filter: {}", 
                reconciliationId, totalFileSize, useBloomFilter);

        if (useBloomFilter) {
            compareWithBloomFilter(reconciliationId, leftTask, rightTask, columnsMapping);
        } else {
            compareWithExactMatching(reconciliationId, leftTask, rightTask, columnsMapping);
        }
    }

    private void compareWithExactMatching(String reconciliationId, Task leftTask, Task rightTask, 
                                        Map<String, String> columnsMapping) throws SQLException {
        LOGGER.info("Using exact matching for reconciliation: {}", reconciliationId);

        List<String> leftColumns = new ArrayList<>();
        List<String> rightColumns = new ArrayList<>();

        AtomicInteger leftLineNumber = new AtomicInteger(0);
        AtomicInteger rightLineNumber = new AtomicInteger(0);

        for (Map.Entry<String, String> entry : columnsMapping.entrySet()) {
            leftColumns.add(entry.getKey());
            rightColumns.add(entry.getValue());
        }

        // Local variables for storing row data with counts
        Map<String, Integer> leftRowCounts = new HashMap<>();
        Map<String, Integer> rightRowCounts = new HashMap<>();

        Map<String, Integer> leftColumnIndices = new HashMap<>();

        s3Service.readFileLineByLine(leftTask.getOutputLocation(), line -> {
            int lineNum = leftLineNumber.incrementAndGet();

            if (lineNum == 1) {
                leftColumnIndices.putAll(parseHeaderAndFindIndices(line, leftColumns));
                LOGGER.debug("Left file column indices: {}", leftColumnIndices);
            } else if (!line.trim().isEmpty()) {
                Map<String, String> rowData = parseDataRow(line, leftColumnIndices);
                if (!rowData.isEmpty()) {
                    String rowKey = leftColumns.stream()
                            .map(col -> rowData.getOrDefault(col, ""))
                            .collect(Collectors.joining("|"));
                    leftRowCounts.merge(rowKey, 1, Integer::sum);
                }
            }
        });

        Map<String, Integer> rightColumnIndices = new HashMap<>();

        s3Service.readFileLineByLine(rightTask.getOutputLocation(), line -> {
            int lineNum = rightLineNumber.incrementAndGet();

            if (lineNum == 1) {
                rightColumnIndices.putAll(parseHeaderAndFindIndices(line, rightColumns));
                LOGGER.debug("Right file column indices: {}", rightColumnIndices);
            } else if (!line.trim().isEmpty()) {
                Map<String, String> rowData = parseDataRow(line, rightColumnIndices);
                if (!rowData.isEmpty()) {
                    String rowKey = rightColumns.stream()
                            .map(col -> rowData.getOrDefault(col, ""))
                            .collect(Collectors.joining("|"));
                    rightRowCounts.merge(rowKey, 1, Integer::sum);
                }
            }
        });

        CSVComparisonResult result = new CSVComparisonResult();

        Set<String> allRowKeys = new HashSet<>(leftRowCounts.keySet());
        allRowKeys.addAll(rightRowCounts.keySet());

        int totalLeftRows = leftRowCounts.values().stream().mapToInt(Integer::intValue).sum();
        int totalRightRows = rightRowCounts.values().stream().mapToInt(Integer::intValue).sum();

        int commonUniqueRows = 0;
        int leftExclusiveUniqueRows = 0;
        int rightExclusiveUniqueRows = 0;

        List<String> sampleExclusiveLeftRows = new ArrayList<>();
        List<String> sampleExclusiveRightRows = new ArrayList<>();
        List<String> sampleCommonRows = new ArrayList<>();

        for (String rowKey : allRowKeys) {
            Integer leftCount = leftRowCounts.getOrDefault(rowKey, 0);
            Integer rightCount = rightRowCounts.getOrDefault(rowKey, 0);

            if (leftCount > 0 && rightCount > 0) {
                // Row exists in both files
                commonUniqueRows++;
                if (sampleCommonRows.size() < 100) {
                    sampleCommonRows.add(rowKey + " (Left: " + leftCount + ", Right: " + rightCount + ")");
                }
            } else if (leftCount > 0) {
                // Row only in left file
                leftExclusiveUniqueRows++;
                if (sampleExclusiveLeftRows.size() < 100) {
                    sampleExclusiveLeftRows.add(rowKey + " (Count: " + leftCount + ")");
                }
            } else {
                // Row only in right file
                rightExclusiveUniqueRows++;
                if (sampleExclusiveRightRows.size() < 100) {
                    sampleExclusiveRightRows.add(rowKey + " (Count: " + rightCount + ")");
                }
            }
        }

        result.setLeftFileRowCount(totalLeftRows);
        result.setRightFileRowCount(totalRightRows);
        result.setCommonRowCount(commonUniqueRows);
        result.setLeftFileExclusiveRowCount(leftExclusiveUniqueRows);
        result.setRightFileExclusiveRowCount(rightExclusiveUniqueRows);

        // Write sample data to S3 and store paths (only for exact matching)
        try {
            String sampleCommonRowsS3Key = writeSampleDataToS3(sampleCommonRows, reconciliationId, "common");
            String sampleExclusiveLeftS3Key = writeSampleDataToS3(sampleExclusiveLeftRows, reconciliationId, "exclusive_left");
            String sampleExclusiveRightS3Key = writeSampleDataToS3(sampleExclusiveRightRows, reconciliationId, "exclusive_right");

            // Only set S3 paths if data was actually written (not null)
            if (sampleCommonRowsS3Key != null) {
                String bucketName = s3Service.getBucketName();
                String sampleCommonRowsS3Path = String.format("s3://%s/%s", bucketName, sampleCommonRowsS3Key);
                result.setSampleCommonRowsS3Path(sampleCommonRowsS3Path);
            }
            
            if (sampleExclusiveLeftS3Key != null) {
                String bucketName = s3Service.getBucketName();
                String sampleExclusiveLeftS3Path = String.format("s3://%s/%s", bucketName, sampleExclusiveLeftS3Key);
                result.setSampleExclusiveLeftRowsS3Path(sampleExclusiveLeftS3Path);
            }
            
            if (sampleExclusiveRightS3Key != null) {
                String bucketName = s3Service.getBucketName();
                String sampleExclusiveRightS3Path = String.format("s3://%s/%s", bucketName, sampleExclusiveRightS3Key);
                result.setSampleExclusiveRightRowsS3Path(sampleExclusiveRightS3Path);
            }

            LOGGER.info("Sample data written to S3 - Common: {}, Left Only: {}, Right Only: {}",
                    sampleCommonRowsS3Key != null ? "s3://" + s3Service.getBucketName() + "/" + sampleCommonRowsS3Key : "null",
                    sampleExclusiveLeftS3Key != null ? "s3://" + s3Service.getBucketName() + "/" + sampleExclusiveLeftS3Key : "null",
                    sampleExclusiveRightS3Key != null ? "s3://" + s3Service.getBucketName() + "/" + sampleExclusiveRightS3Key : "null");
        } catch (Exception e) {
            LOGGER.error("Failed to write sample data to S3: {}", e.getMessage(), e);
            // Continue without sample data - reconciliation still succeeds
        }

        LOGGER.info("Exact matching reconciliation completed - Left Total: {}, Right Total: {}, Common Unique: {}, Left Only Unique: {}, Right Only Unique: {}",
                totalLeftRows, totalRightRows, commonUniqueRows,
                leftExclusiveUniqueRows, rightExclusiveUniqueRows);

        // Store the result in the database
        try {
            reconciliationResultsDAO.upsertReconciliationResult(reconciliationId, result, "SUCCESS", "EXACT_MATCH");
            LOGGER.info("Exact matching reconciliation result stored in database for ID: {}", reconciliationId);
        } catch (SQLException e) {
            LOGGER.error("Failed to store exact matching reconciliation result in database for ID: {}", reconciliationId, e);
            storeFailedResult(reconciliationId);
            throw e;
        }
    }

    private void compareWithBloomFilter(String reconciliationId, Task leftTask, Task rightTask, 
                                      Map<String, String> columnsMapping) throws SQLException {
        LOGGER.info("Using Bloom filter for reconciliation: {}", reconciliationId);

        List<String> leftColumns = new ArrayList<>();
        List<String> rightColumns = new ArrayList<>();

        for (Map.Entry<String, String> entry : columnsMapping.entrySet()) {
            leftColumns.add(entry.getKey());
            rightColumns.add(entry.getValue());
        }

        // Create Bloom filters
        InMemoryBloomFilter<String> leftRowFilter = new InMemoryBloomFilter<>(reconciliationConfig.getEstimatedRows(), reconciliationConfig.getFalsePositiveRate());
        InMemoryBloomFilter<String> rightRowFilter = new InMemoryBloomFilter<>(reconciliationConfig.getEstimatedRows(), reconciliationConfig.getFalsePositiveRate());

        // First pass: Build Bloom filters
        buildBloomFilter(leftTask.getOutputLocation(), leftColumns, leftRowFilter, "left");
        buildBloomFilter(rightTask.getOutputLocation(), rightColumns, rightRowFilter, "right");

        // Second pass: Cross-compare
        CSVComparisonResult result = crossCompareWithBloomFilters(
            leftTask.getOutputLocation(), rightTask.getOutputLocation(),
            leftColumns, rightColumns, leftRowFilter, rightRowFilter);

        // Store result in database
        try {
            reconciliationResultsDAO.upsertReconciliationResult(reconciliationId, result, "SUCCESS", "PROBABILISTIC_MATCH");
            LOGGER.info("Bloom filter reconciliation result stored in database for ID: {}", reconciliationId);
        } catch (SQLException e) {
            LOGGER.error("Failed to store Bloom filter reconciliation result in database for ID: {}", reconciliationId, e);
            storeFailedResult(reconciliationId);
            throw e;
        }
    }

    private void buildBloomFilter(String s3Path, List<String> columns, InMemoryBloomFilter<String> bloomFilter, String fileType) {
        Map<String, Integer> columnIndices = new HashMap<>();
        AtomicInteger lineNumber = new AtomicInteger(0);

        s3Service.readFileLineByLine(s3Path, line -> {
            int lineNum = lineNumber.incrementAndGet();

            if (lineNum == 1) {
                // Parse header
                columnIndices.putAll(parseHeaderAndFindIndices(line, columns));
                LOGGER.debug("{} file column indices: {}", fileType, columnIndices);
            } else if (!line.trim().isEmpty()) {
                // Parse data row and add to Bloom filter
                Map<String, String> rowData = parseDataRow(line, columnIndices);
                if (!rowData.isEmpty()) {
                    String rowKey = columns.stream()
                            .map(col -> rowData.getOrDefault(col, ""))
                            .collect(Collectors.joining("|"));
                    bloomFilter.add(rowKey);
                }
            }
        });

        LOGGER.info("Built Bloom filter for {} file with {} rows", fileType, lineNumber.get() - 1);
    }

    private CSVComparisonResult crossCompareWithBloomFilters(String leftPath, String rightPath,
                                                           List<String> leftColumns, List<String> rightColumns,
                                                           InMemoryBloomFilter<String> leftRowFilter, InMemoryBloomFilter<String> rightRowFilter) {
        CSVComparisonResult result = new CSVComparisonResult();

        int leftTotalRows = 0;
        AtomicInteger leftToRightMatches = new AtomicInteger();

        Map<String, Integer> leftColumnIndices = new HashMap<>();
        AtomicInteger leftLineNumber = new AtomicInteger(0);

        s3Service.readFileLineByLine(leftPath, line -> {
            int lineNum = leftLineNumber.incrementAndGet();

            if (lineNum == 1) {
                leftColumnIndices.putAll(parseHeaderAndFindIndices(line, leftColumns));
            } else if (!line.trim().isEmpty()) {
                Map<String, String> rowData = parseDataRow(line, leftColumnIndices);
                if (!rowData.isEmpty()) {
                    String rowKey = leftColumns.stream()
                            .map(col -> rowData.getOrDefault(col, ""))
                            .collect(Collectors.joining("|"));
                    
                    if (rightRowFilter.contains(rowKey)) {
                        leftToRightMatches.getAndIncrement();
                    }
                }
            }
        });

        leftTotalRows = leftLineNumber.get() - 1;

        int rightTotalRows = 0;
        AtomicInteger rightToLeftMatches = new AtomicInteger();

        Map<String, Integer> rightColumnIndices = new HashMap<>();
        AtomicInteger rightLineNumber = new AtomicInteger(0);

        s3Service.readFileLineByLine(rightPath, line -> {
            int lineNum = rightLineNumber.incrementAndGet();

            if (lineNum == 1) {
                rightColumnIndices.putAll(parseHeaderAndFindIndices(line, rightColumns));
            } else if (!line.trim().isEmpty()) {
                Map<String, String> rowData = parseDataRow(line, rightColumnIndices);
                if (!rowData.isEmpty()) {
                    String rowKey = rightColumns.stream()
                            .map(col -> rowData.getOrDefault(col, ""))
                            .collect(Collectors.joining("|"));
                    
                    if (leftRowFilter.contains(rowKey)) {
                        rightToLeftMatches.getAndIncrement();
                    }
                }
            }
        });

        rightTotalRows = rightLineNumber.get() - 1;

        int commonRows = Math.min(leftToRightMatches.get(), rightToLeftMatches.get());
        int leftExclusiveRows = leftTotalRows - commonRows;
        int rightExclusiveRows = rightTotalRows - commonRows;

        LOGGER.info("Bloom filter bidirectional matching - Left→Right: {}, Right→Left: {}, Using minimum: {}",
                leftToRightMatches.get(), rightToLeftMatches.get(), commonRows);

        result.setLeftFileRowCount(leftTotalRows);
        result.setRightFileRowCount(rightTotalRows);
        result.setCommonRowCount(commonRows);
        result.setLeftFileExclusiveRowCount(leftExclusiveRows);
        result.setRightFileExclusiveRowCount(rightExclusiveRows);

        result.setSampleCommonRowsS3Path(null);
        result.setSampleExclusiveLeftRowsS3Path(null);
        result.setSampleExclusiveRightRowsS3Path(null);

        LOGGER.info("Bloom filter reconciliation completed - Left Total: {}, Right Total: {}, Common: {}, Left Only: {}, Right Only: {} (99% accurate)", 
                leftTotalRows, rightTotalRows, commonRows, leftExclusiveRows, rightExclusiveRows);

        return result;
    }

    private void storeFailedResult(String reconciliationId) throws SQLException {
        try {
            CSVComparisonResult failedResult = new CSVComparisonResult();
            reconciliationResultsDAO.upsertReconciliationResult(reconciliationId, failedResult, "FAILED", "ADAPTIVE_MATCH");
            LOGGER.info("Failed reconciliation result stored in database for ID: {}", reconciliationId);
        } catch (SQLException e) {
            LOGGER.error("Failed to store failed reconciliation result in database for ID: {}", reconciliationId, e);
            throw e;
        }
    }

    private Map<String, String> parseDataRow(String dataLine, Map<String, Integer> columnIndices) {
        Map<String, String> rowData = new HashMap<>();

        try (CSVReader csvReader = new CSVReader(new StringReader(dataLine))) {
            String[] columns = csvReader.readNext();
            if (columns != null) {
                for (Map.Entry<String, Integer> entry : columnIndices.entrySet()) {
                    String columnName = entry.getKey();
                    int columnIndex = entry.getValue();

                    if (columnIndex < columns.length) {
                        rowData.put(columnName, columns[columnIndex].trim());
                    } else {
                        LOGGER.warn("Column index {} out of bounds for column {}", columnIndex, columnName);
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.error("Error parsing data line: {}", e.getMessage());
        }

        return rowData;
    }

    private Map<String, Integer> parseHeaderAndFindIndices(String headerLine, List<String> neededColumns) {
        Map<String, Integer> columnIndices = new HashMap<>();

        try (CSVReader csvReader = new CSVReader(new StringReader(headerLine))) {
            String[] headers = csvReader.readNext();
            if (headers != null) {
                for (int i = 0; i < headers.length; i++) {
                    String header = headers[i].trim();
                    if (neededColumns.contains(header)) {
                        columnIndices.put(header, i);
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.error("Error parsing header line: {}", e.getMessage());
        }

        return columnIndices;
    }

    private Map<String, String> transformToMap(String mapping) {
        try {
            if (mapping == null || mapping.trim().isEmpty()) {
                LOGGER.warn("Empty mapping JSON provided");
                return new HashMap<>();
            }
            
            return GSON.fromJson(mapping, new TypeToken<Map<String, String>>(){}.getType());
        } catch (Exception e) {
            LOGGER.error("Failed to parse mapping JSON: {}", e.getMessage(), e);
            return new HashMap<>();
        }
    }
    
    private String writeSampleDataToS3(List<String> sampleData, String reconciliationId, String type) {
        try {
            if (sampleData == null || sampleData.isEmpty()) {
                LOGGER.debug("No sample data to write for type: {}", type);
                return null;
            }
            
            // Create filename for reconciliation samples
            String fileName = String.format("reconciliation_samples/%s/%s_%s.json", 
                reconciliationId, type, System.currentTimeMillis());
            
            String jsonData = GSON.toJson(sampleData);
            
            // Use S3Service to write the reconciliation output file
            String s3Path = s3Service.writeReconciliationOutput(jsonData, fileName);
            LOGGER.debug("Sample data written to S3: {} (type: {}, rows: {})", s3Path, type, sampleData.size());
            
            return s3Path;
        } catch (Exception e) {
            LOGGER.error("Failed to write sample data to S3 for type {}: {}", type, e.getMessage(), e);
            throw new RuntimeException("Failed to write sample data to S3", e);
        }
    }
}
