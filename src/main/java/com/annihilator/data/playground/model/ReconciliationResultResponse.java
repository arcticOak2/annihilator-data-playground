package com.annihilator.data.playground.model;

import java.sql.Timestamp;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ReconciliationResultResponse {

  private String reconciliationId;
  private String status;
  private Timestamp executionTimestamp;
  private String reconciliationMethod;

  // Count fields from CSVComparisonResult
  private int leftFileRowCount;
  private int rightFileRowCount;
  private int commonRowCount;
  private int leftFileExclusiveRowCount;
  private int rightFileExclusiveRowCount;

  // S3 paths for sample data
  private String sampleCommonRowsS3Path;
  private String sampleExclusiveLeftRowsS3Path;
  private String sampleExclusiveRightRowsS3Path;
}
