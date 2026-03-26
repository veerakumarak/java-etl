package io.github.veerakumarak.etl.compare;

import io.github.veerakumarak.etl.utils.FileType;
import io.github.veerakumarak.etl.utils.FileTypeDetector;
import io.github.veerakumarak.fp.Result;
import io.github.veerakumarak.fp.failures.IllegalArgument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

public class DataComparator<T> {

    private static final Logger log = LoggerFactory.getLogger(DataComparator.class);

    private static Result<FileType> validateAndDetectFileType(String path1, String path2) {
        log.info("Starting comparison of {} and {}", path1, path2);
        return Result.of(() -> {
            FileType fileType1 = FileTypeDetector.detectType(path1).orElseThrow();
            FileType fileType2 = FileTypeDetector.detectType(path2).orElseThrow();
            if (fileType1 != fileType2) {
                throw new IllegalArgument("File types do not match");
            }
            if (fileType1 != FileType.PARQUET) {
                throw new IllegalArgument("Unsupported file type: " + fileType1);
            }
            return fileType1;
        });
    }

    public static Result<ComparisonResult> ordered(String path1, String path2, Set<String> ignoredColumns) {
        return Result.of(() -> {
            validateAndDetectFileType(path1, path2).orElseThrow();
            return ParquetComparator.ordered(path1, path2, ignoredColumns).orElseThrow();
        });
    }

    public static <T> Result<ComparisonResult> ordered(String path1, String path2, Class<T> tClass) {
        return Result.of(() -> {
            validateAndDetectFileType(path1, path2).orElseThrow();
            return ParquetComparator.ordered(path1, path2, tClass).orElseThrow();
        });
    }

    public static Result<ComparisonResult> unorderedBuffered(String path1, String path2, Set<String> ignoredColumns, Long bufferSize) {
        return Result.of(() -> {
            validateAndDetectFileType(path1, path2).orElseThrow();
            return ParquetComparator.unorderedBuffered(path1, path2, ignoredColumns, bufferSize).orElseThrow();
        });
    }

    public static <T> Result<ComparisonResult> unorderedBuffered(String path1, String path2, Class<T> tClass, Long bufferSize) {
        return Result.of(() -> {
            validateAndDetectFileType(path1, path2).orElseThrow();
            return ParquetComparator.unorderedBuffered(path1, path2, tClass, bufferSize).orElseThrow();
        });
    }

    public static Result<ComparisonResult> unordered(String path1, String path2, Set<String> ignoredColumns) {
        return unorderedBuffered(path1, path2, ignoredColumns, Long.MAX_VALUE);
    }

    public static <T> Result<ComparisonResult> unordered(String path1, String path2, Class<T> tClass) {
        return unorderedBuffered(path1, path2, tClass, Long.MAX_VALUE);
    }

//    public static Result<ComparisonResult> unorderedBucketised(String path1, String path2, Set<String> ignoredColumns) {
//        return Result.of(() -> {
//            validateAndDetectFileType(path1, path2).orElseThrow();
//            return ParquetComparator.unorderedBucketised(path1, path2, ignoredColumns).orElseThrow();
//        });
//    }
//
//    public static <T> Result<ComparisonResult> unorderedBucketised(String path1, String path2, Class<T> tClass) {
//        return Result.of(() -> {
//            validateAndDetectFileType(path1, path2).orElseThrow();
//            return ParquetComparator.unorderedBucketised(path1, path2, tClass).orElseThrow();
//        });
//    }

}
