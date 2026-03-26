package io.github.veerakumarak.etl.compare;

import io.github.veerakumarak.etl.source.ParquetReaderHelper;
import io.github.veerakumarak.fp.Result;
import io.github.veerakumarak.fp.failures.IllegalState;
import org.apache.commons.io.FileUtils;
import org.apache.parquet.example.data.Group;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Stream;

public class ParquetComparator {

    private static final Logger log = LoggerFactory.getLogger(ParquetComparator.class);

    public static Result<ComparisonResult> ordered(String path1, String path2, Set<String> ignoredColumns) {
        return Result.of(() -> {
            try (Stream<Group> s1 = ParquetReaderHelper.readStream(path1).orElseThrow();
                 Stream<Group> s2 = ParquetReaderHelper.readStream(path2).orElseThrow()) {

                return compareOrdered(s1.iterator(), s2.iterator(), GroupHelper.getComparator(ignoredColumns));
            }
        });
    }

    public static <T> Result<ComparisonResult> ordered(String path1, String path2, Class<T> tClass) {
        return Result.of(() -> {
            try (Stream<T> s1 = ParquetReaderHelper.readStream(path1, tClass).orElseThrow();
                 Stream<T> s2 = ParquetReaderHelper.readStream(path2, tClass).orElseThrow()) {

                return compareOrdered(s1.iterator(), s2.iterator(), TypedHelper.getComparator());
            }
        });
    }

    public static <T> ComparisonResult compareOrdered(Iterator<T> iter1, Iterator<T> iter2, Comparator<T> comparator) {
        int count = 0;
        long mismatchCount = 0;
        while (iter1.hasNext() && iter2.hasNext()) {
            count++;
            if (comparator.compare(iter1.next(), iter2.next()) != 0) {
                mismatchCount++;
            }
        }

        int count1 = count;
        while (iter1.hasNext()) {
            count1++;
            iter1.next();
        }
        int count2 = count;
        while (iter2.hasNext()) {
            count2++;
            iter2.next();
        }
        boolean result = (count1 == count2) && (mismatchCount == 0);
        return result ? ComparisonResult.match(count1) : ComparisonResult.mismatch(count1, count2, mismatchCount, "Ordered data mismatch");
    }

    public static Result<ComparisonResult> unorderedBuffered(String path1, String path2, Set<String> ignoredColumns, Long bufferSize) {
        return Result.of(() -> {
            try (Stream<Group> s1 = ParquetReaderHelper.readStream(path1).orElseThrow();
                 Stream<Group> s2 = ParquetReaderHelper.readStream(path2).orElseThrow()) {
                return compareUnorderedBuffered(s1.iterator(), s2.iterator(), bufferSize, GroupHelper.getHashFunction(ignoredColumns));
            }
        });
    }

    public static <T> Result<ComparisonResult> unorderedBuffered(String path1, String path2, Class<T> tClass, Long bufferSize) {
        return Result.of(() -> {
            try (Stream<T> s1 = ParquetReaderHelper.readStream(path1, tClass).orElseThrow();
                 Stream<T> s2 = ParquetReaderHelper.readStream(path2, tClass).orElseThrow()) {
                return compareUnorderedBuffered(s1.iterator(), s2.iterator(), bufferSize, TypedHelper.getHashFunction());
            }
        });
    }

    private static <T> ComparisonResult compareUnorderedBuffered(Iterator<T> it1, Iterator<T> it2, Long bufferSize, Function<T, Integer> hashFunction) {
        int count1 = 0;
        int count2 = 0;

        Map<Integer, Long> diffMap = new HashMap<>();

        while (it1.hasNext() || it2.hasNext()) {
            if (it1.hasNext()) {
                count1++;
                T val = it1.next();
                int hash = hashFunction.apply(val);
                diffMap.compute(hash, (k, v) -> {
                    long newVal = (v == null) ? 1L : v + 1L;
                    return (newVal == 0) ? null : newVal; // Remove if balanced
                });
            }
            if (it2.hasNext()) {
                count2++;
                T val = it2.next();
                int hash = hashFunction.apply(val);
                diffMap.compute(hash, (k, v) -> {
                    long newVal = (v == null) ? -1L : v - 1L;
                    return (newVal == 0) ? null : newVal; // Remove if balanced
                });
            }
            if (bufferSize <= diffMap.size()) {
                throw new IllegalState("Buffer size exceeded");
            }
        }

        long mismatchCount = diffMap.values().stream().filter(v -> v != 0L).count();
        boolean result = (count1 == count2) && (mismatchCount == 0) && diffMap.isEmpty();
        return result ? ComparisonResult.match(count1) : ComparisonResult.mismatch(count1, count2, mismatchCount, "Unordered data mismatch");
    }

//    public static Result<ComparisonResult> unorderedBucketised(String path1, String path2, Set<String> ignoredColumns) {
//        return compareUnorderedBucketised(path1, path2, GroupHelper.getHashFunction(ignoredColumns));
//    }
//
//    public static <T> Result<ComparisonResult> unorderedBucketised(String path1, String path2, Class<T> tClass) {
//        return compareUnorderedBucketised(path1, path2, tClass, Object::hashCode);
//    }

//    public static Result<ComparisonResult> compareUnorderedBucketised(String pathA, String pathB, Function<Group, Integer> hashFunction) {
//        return Result.of(() -> {
//            log.info("Starting partitioned comparison with {} buckets", BucketPartitioner.NO_OF_BUCKETS);
//
//            try {
//                // Step 1: Partition both files into hashed buckets
//                Map<Integer, List<String>> bucketsA = BucketPartitioner.partition(pathA, UUID.randomUUID().toString(), hashFunction).orElseThrow();
//                Map<Integer, List<String>> bucketsB = BucketPartitioner.partition(pathB, UUID.randomUUID().toString(), hashFunction).orElseThrow();
//
//                // Step 2: Compare each bucket pair
//                ComparisonResult bucketResult = ComparisonResult.match(0);
//                for (int i = 0; i < BucketPartitioner.NO_OF_BUCKETS; i++) {
//                    List<String> bA = bucketsA.get(i);
//                    List<String> bB = bucketsB.get(i);
//                    ComparisonResult newResult = compareBucketPair(bA, bB).orElseThrow();
//                    bucketResult = bucketResult.combine(newResult);
////                    if (!bucketResult.isMatch()) {
////                        return bucketResult; // Exit early on first mismatch
////                    }
//                }
//                return bucketResult;
//            } finally {
//                // 4. Cleanup: CRITICAL for K8s to avoid DiskFull errors
//                FileUtils.deleteQuietly(new File(BucketPartitioner.TEMP_DIR + pathA));
//                log.info("Cleaned up temporary buckets at {}", BucketPartitioner.TEMP_DIR + pathA);
//                FileUtils.deleteQuietly(new File(BucketPartitioner.TEMP_DIR + pathB));
//                log.info("Cleaned up temporary buckets at {}", BucketPartitioner.TEMP_DIR + pathB);
//            }
//        });
//    }

//    public static <T> Result<ComparisonResult> compareUnorderedBucketised(String pathA, String pathB, Class<T> tClass, Function<T, Integer> hashFunction) {
//        return Result.of(() -> {
//            log.info("Starting partitioned comparison with {} buckets", BucketPartitioner.NO_OF_BUCKETS);
//
//            try {
//                // Step 1: Partition both files into hashed buckets
//                Map<Integer, List<String>> bucketsA = BucketPartitioner.partition(pathA, UUID.randomUUID().toString(), hashFunction).orElseThrow();
//                Map<Integer, List<String>> bucketsB = BucketPartitioner.partition(pathB, UUID.randomUUID().toString(), hashFunction).orElseThrow();
//
//                // Step 2: Compare each bucket pair
//                ComparisonResult bucketResult = ComparisonResult.match(0);
//                for (int i = 0; i < BucketPartitioner.NO_OF_BUCKETS; i++) {
//                    List<String> bA = bucketsA.get(i);
//                    List<String> bB = bucketsB.get(i);
//                    ComparisonResult newResult = compareBucketPair(bA, bB, tClass).orElseThrow();
//                    bucketResult = bucketResult.combine(newResult);
////                    if (!bucketResult.isMatch()) {
////                        return bucketResult; // Exit early on first mismatch
////                    }
//                }
//                return bucketResult;
//            } finally {
//                // 4. Cleanup: CRITICAL for K8s to avoid DiskFull errors
//                FileUtils.deleteQuietly(new File(BucketPartitioner.TEMP_DIR + pathA));
//                log.info("Cleaned up temporary buckets at {}", BucketPartitioner.TEMP_DIR + pathA);
//                FileUtils.deleteQuietly(new File(BucketPartitioner.TEMP_DIR + pathB));
//                log.info("Cleaned up temporary buckets at {}", BucketPartitioner.TEMP_DIR + pathB);
//            }
//        });
//    }

//    private static <T> Result<ComparisonResult> compareBucketPair(List<String> paths1, List<String> paths2) {
//        return Result.of(() -> {
//            boolean empty1 = (paths1 == null || paths1.isEmpty());
//            boolean empty2 = (paths2 == null || paths2.isEmpty());
//
//            if (empty1 && empty2) return ComparisonResult.match(0);
//            if (empty1 || empty2) {
//                return ComparisonResult.mismatch(0L, 0L, 0L, "Bucket part files missing in one side");
//            }
//
//            try (Stream<T> s1 = paths1.stream()
//                    .flatMap(path -> ParquetReaderHelper.readStream(path, tClass).orElse(Stream.empty()));
//                 Stream<T> s2 = paths2.stream()
//                         .flatMap(path -> ParquetReaderHelper.readStream(path, tClass).orElse(Stream.empty()))) {
//
//                return compareUnorderedBuffered(s1.iterator(), s2.iterator(), Long.MAX_VALUE, null);
//            }
//        });
//    }

//    private static <T> Result<ComparisonResult> compareBucketPair(List<String> paths1, List<String> paths2, Class<T> tClass) {
//        return Result.of(() -> {
//            boolean empty1 = (paths1 == null || paths1.isEmpty());
//            boolean empty2 = (paths2 == null || paths2.isEmpty());
//
//            if (empty1 && empty2) return ComparisonResult.match(0);
//            if (empty1 || empty2) {
//                return ComparisonResult.mismatch(0L, 0L, 0L, "Bucket part files missing in one side");
//            }
//
//            try (Stream<T> s1 = paths1.stream()
//                    .flatMap(path -> ParquetReaderHelper.readStream(path, tClass).orElse(Stream.empty()));
//                 Stream<T> s2 = paths2.stream()
//                         .flatMap(path -> ParquetReaderHelper.readStream(path, tClass).orElse(Stream.empty()))) {
//
//                return compareUnorderedBuffered(s1.iterator(), s2.iterator(), Long.MAX_VALUE, null);
//            }
//        });
//    }

}
