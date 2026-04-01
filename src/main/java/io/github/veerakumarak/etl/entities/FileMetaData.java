package io.github.veerakumarak.etl.entities;

import java.util.Map;
import java.util.Set;

public record FileMetaData(
        String baseOutputPath,          // The root S3/HDFS directory
        Map<String, Long> partitionCounts, // e.g., {"region=US": 5000, "region=EU": 3000}
        Set<String> generatedFiles      // List of all individual .parquet file paths
) {
}
