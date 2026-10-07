package io.github.veerakumarak.etl.sink;

import io.github.veerakumarak.etl.entities.FileMetaData;
import io.github.veerakumarak.etl.utils.FileType;
import io.github.veerakumarak.fp.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.time.ZoneId;
import java.util.List;
import java.util.stream.Stream;

public class DataSink {

    private static final Logger log = LoggerFactory.getLogger(DataSink.class);

    public static Result<FileMetaData> write(String writePath, FileType fileType, String tableName, ResultSet resultSet, Integer batchSize, List<String> partitionKeys) {
        return write(writePath, fileType, tableName, resultSet, batchSize, partitionKeys, false, ZoneId.systemDefault());
    }

    /**
     * Writes a {@link ResultSet} to the given file format.
     *
     * @param int96Timestamps when {@code true} and writing Parquet, TIMESTAMP columns use the legacy INT96
     *                        format (for older Spark readers) instead of INT64. Opt-in; INT96 is deprecated.
     * @param zoneId          time zone used to encode INT96 timestamps (ignored otherwise).
     */
    public static Result<FileMetaData> write(String writePath, FileType fileType, String tableName, ResultSet resultSet, Integer batchSize, List<String> partitionKeys, boolean int96Timestamps, ZoneId zoneId) {
        if (fileType == FileType.PARQUET) {
            return ParquetWriterHelper.writeBatched(writePath, tableName, resultSet, batchSize, partitionKeys, int96Timestamps, zoneId);
        } else if (fileType == FileType.CSV) {
            return CsvWriterHelper.writeBatched(writePath, tableName, resultSet, batchSize, partitionKeys);
        }
        log.error("Unsupported output path: " + writePath);
        return Result.failure("Unsupported file format provided");
    }

    public static <T> Result<FileMetaData> writeStream(String writePath, FileType fileType, String tableName, Integer batchSize, Stream<T> data, Class<T> tClass, List<String> partitionKeys) {
        return writeStream(writePath, fileType, tableName, batchSize, data, tClass, partitionKeys, false, ZoneId.systemDefault());
    }

    /**
     * Streams POJOs to the given file format.
     *
     * @param int96Timestamps when {@code true} and writing Parquet, {@code LocalDateTime} fields use the
     *                        legacy INT96 format (for older Spark readers) instead of INT64. Opt-in.
     * @param zoneId          time zone used to encode INT96 timestamps (ignored otherwise).
     */
    public static <T> Result<FileMetaData> writeStream(String writePath, FileType fileType, String tableName, Integer batchSize, Stream<T> data, Class<T> tClass, List<String> partitionKeys, boolean int96Timestamps, ZoneId zoneId) {
        if (fileType == FileType.PARQUET) {
            return ParquetWriterHelper.writeBatched(writePath, tableName, batchSize, data, tClass, partitionKeys, int96Timestamps, zoneId);
        } else if (fileType == FileType.CSV) {
//            return CsvWriterHelper.writeBatched(writePath, tableName, batchSize, data, tClass, partitionKeys);
        }
        log.error("Unsupported output path: " + writePath);
        return Result.failure("Unsupported file format provided");
    }

}
