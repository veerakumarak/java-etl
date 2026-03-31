package io.github.veerakumarak.etl.sink;

import io.github.veerakumarak.etl.entities.FileMetaData;
import io.github.veerakumarak.fp.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.util.List;
import java.util.stream.Stream;

public class DataSink {

    private static final Logger log = LoggerFactory.getLogger(DataSink.class);

    public static Result<FileMetaData> write(String outputPath, String tableName, ResultSet resultSet, Integer batchSize) {
        if (outputPath.endsWith(".parquet")) {
            return ParquetWriterHelper.writeBatched(outputPath, tableName, resultSet, batchSize);
        }
//        else if (outputPath.endsWith(".csv")) {
//            return toCsv(path, rs, batchSize);
//        }
        log.error("Unsupported output path: " + outputPath);
        return Result.failure("Unsupported format");
    }

    public static <T> Result<FileMetaData> writeStream(String outputPath, String tableName, Integer batchSize, Stream<T> data, Class<T> tClass) {
        if (outputPath.endsWith(".parquet")) {
            return ParquetWriterHelper.writeBatched(outputPath, tableName, batchSize, data, tClass);
        }
//        else if (outputPath.endsWith(".csv")) {
//            return toCsv(path, rs, batchSize);
//        }
        log.error("Unsupported output path: " + outputPath);
        return Result.failure("Unsupported format");
    }

}
