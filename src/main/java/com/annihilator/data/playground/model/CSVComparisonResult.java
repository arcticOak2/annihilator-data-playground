package com.annihilator.data.playground.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class CSVComparisonResult {

    private int leftFileRowCount;
    private int rightFileRowCount;
    private int commonRowCount;
    private int leftFileExclusiveRowCount;
    private int rightFileExclusiveRowCount;
    private String sampleCommonRowsS3Path;
    private String sampleExclusiveLeftRowsS3Path;
    private String sampleExclusiveRightRowsS3Path;
}
