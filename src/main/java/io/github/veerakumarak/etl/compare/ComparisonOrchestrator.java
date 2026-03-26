//package io.github.veerakumarak.etl.compare;
//
//import io.github.veerakumarak.fp.Result;
//import org.apache.commons.io.FileUtils;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.File;
//import java.util.UUID;
//
//public class ComparisonOrchestrator {
//
//    private static final Logger log = LoggerFactory.getLogger(ComparisonOrchestrator.class);
//
//    public <T> Result<ComparisonResult> run(String pathA, String pathB, Class<T> tClass) {
//        // 1. Initialize Temp Directory (K8s emptyDir)
//        String tempPath = "/tmp/buckets/" + UUID.randomUUID();
//        new File(tempPath).mkdirs();
//
//        try {
//            // 2. Partition Phase: Break huge files into N manageable pieces
//            // We use 64 buckets as a good balance for 16GB RAM
//            DataComparator<T> comparator = new DataComparator<>(
//                    pathA, pathB, tClass, 64, tempPath
//            );
//
//            log.info("Starting Partitioned Comparison...");
//            Result<ComparisonResult> result = comparator.compareUnorderedBuffered();
//
//            // 3. Telemetry: Log the outcome for Airflow/Gemini logs
//            result.ifOk(res -> {
//                if (res.isMatch()) {
//                    log.info("SUCCESS: Files are identical across all buckets.");
//                } else {
//                    log.error("FAILURE: Mismatch found. Sample: {}", res.actualValueA());
//                }
//            });
//
//            return result;
//
//        } finally {
//            // 4. Cleanup: CRITICAL for K8s to avoid DiskFull errors
//            FileUtils.deleteQuietly(new File(tempPath));
//            log.info("Cleaned up temporary buckets at {}", tempPath);
//        }
//    }
//}