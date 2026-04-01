package io.github.veerakumarak.etl.entities;

import java.util.Map;
import java.util.Set;

public record ExtractResult(
        String baseOutputPath,          // The root S3/HDFS directory
        long totalCount,                // Total rows processed
        Map<String, Long> partitionCounts, // e.g., {"region=US": 5000, "region=EU": 3000}
        Set<String> generatedFiles      // List of all individual .parquet file paths
) {
    public ExtractResult(FileMetaData fileMetaData) {
        this(fileMetaData.baseOutputPath(),
                fileMetaData.partitionCounts().values().stream().mapToLong(Long::longValue).sum(),
                fileMetaData.partitionCounts(),
                fileMetaData.generatedFiles());
    }
}