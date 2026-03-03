package io.github.veerakumarak.etl.parquet;

import io.github.veerakumarak.etl.entities.FileMetaData;
import io.github.veerakumarak.fp.Pair;
import io.github.veerakumarak.fp.Result;
import io.github.veerakumarak.fp.failures.InternalFailure;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class ParquetWriterHelper {

    private static final Logger log = LoggerFactory.getLogger(ParquetWriterHelper.class);

    private static final String S3A_PREFIX = "s3a://";


    private static Result<Pair<List<Group>, MessageType>> resultSetToGroups(String tableName, ResultSet resultSet, ResultSetMetaData sourceMetaData, ResultSetMetaData targetMetaData) {
        return Result.of(() -> {
            MessageType schema = MessageTypeConverter.getParquetSchemaFromResultSet(tableName, targetMetaData)
                    .orElseThrow(() -> new InternalFailure("Could not get schema from result set"));
            List<Group> groups = GroupConverter.convert(sourceMetaData, targetMetaData, schema, resultSet)
                    .orElseThrow(() -> new InternalFailure("Could not get groups from result set"));
            return Pair.of(groups, schema);
        });
    }

    private static ParquetWriter<Group> createWriter(String path, MessageType schema, Configuration conf) throws IOException {
        return ExampleParquetWriter.builder(new Path(path))
                .withConf(conf)
                .withType(schema)
                .withDictionaryEncoding(true)
                .withValidation(false)
                .withPageWriteChecksumEnabled(false)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .build();
    }

    public static Result<FileMetaData> toFile(String filePath, List<Group> groups, MessageType schema, String partitionPrefix) {
        // Add s3a:// prefix for Hadoop to write to S3, but return path without prefix
        // This maintains backward compatibility with DAGs that add their own prefix
//        String s3Path = S3A_PREFIX + filePath;
        String s3Path = filePath;

        return Result.of(() -> {
            try (ParquetWriter<Group> writer = createWriter(s3Path, schema, ParquetAwsManager.getConfiguration())) {
                for (Group group : groups) {
                    writer.write(group);
                }
            }
            return new FileMetaData(filePath, groups.size(), partitionPrefix);
        });
    }

    public static Result<FileMetaData> toFile(String location, String tableName, ResultSet resultSet) {
        return Result.of(resultSet::getMetaData)
                .flatMap(metadata -> toFile(location, tableName, resultSet, List.of(), metadata, metadata));
    }

    public static Result<FileMetaData> toFile(String location, String tableName, ResultSet resultSet, List<String> partitionKeys, ResultSetMetaData sourceMetaData, ResultSetMetaData targetMetaData) {
        return Result.of(() -> {
            if (Objects.isNull(resultSet)) {
                throw new InternalFailure("ResultSet is null");
            }

            Pair<List<Group>, MessageType> groupsAndSchema = resultSetToGroups(tableName, resultSet, sourceMetaData, targetMetaData)
                    .orElseThrow();
            MessageType schema = groupsAndSchema.getSecond();
            List<Group> groups = groupsAndSchema.getFirst();

            StringBuilder partitionPrefixBuilder = new StringBuilder();
            String columnValue;

            if (!partitionKeys.isEmpty()) {
                for (String key : partitionKeys) {
                    String getOriginalTypes = schema.getFields().get(schema.getFieldIndex(key)).getOriginalType().toString();
                    try {
                        Group obj = groups.getFirst();
                        if (getOriginalTypes.equalsIgnoreCase("date")) {
                            int epochDate = obj.getInteger(key, 0);
                            columnValue = LocalDate.ofEpochDay(epochDate).toString();
                        } else {
                            columnValue = obj.getString(key, 0);
                        }
                        partitionPrefixBuilder.append((!partitionPrefixBuilder.toString().isEmpty()) ? "/" : "").append(key).append("=").append(columnValue);
                    } catch (Exception e) {
                        throw new InternalError("partition key '" + key + "' has null or empty value");
                    }
                }
            }
            String filePath = location + "/" + (partitionPrefixBuilder.toString().isEmpty() ? "" : partitionPrefixBuilder + "/") + tableName + ".parquet";

            return toFile(filePath, groups, schema, partitionPrefixBuilder.toString())
                    .orElseThrow(() -> new InternalFailure("Could not get file from result set"));
        });
    }

    public static Result<FileMetaData> writeBatched(String outputPath, String tableName, ResultSet resultSet,
            Integer batchSize, Configuration conf) {

        if (Objects.isNull(resultSet)) {
            return Result.failure(new InternalFailure("ResultSet is null"));
        }
        if (batchSize == null || batchSize <= 0) {
            return Result.failure(new InternalFailure("Batch size must be positive"));
        }

        return Result.of(() -> {
            ResultSetMetaData metaData = resultSet.getMetaData();
            MessageType schema = MessageTypeConverter.getParquetSchemaFromResultSet(tableName, metaData)
                    .orElseThrow(() -> new InternalFailure("Could not get schema from result set"));

            String fileName = tableName + ".parquet";
            String fullPath = outputPath + "/" + fileName;
            // Add s3a:// prefix for Hadoop to write to S3
            String s3Path = S3A_PREFIX + fullPath;

            long rowCount = 0;
            try (ParquetWriter<Group> writer = createWriter(s3Path, schema, conf)) {
                List<Group> batch = new ArrayList<>(batchSize);
                while (resultSet.next()) {
                    Result<Group> groupTry = GroupConverter.convertSingleRow(schema, metaData, metaData, resultSet);
                    if (groupTry.isFailure()) {
                        throw new Exception("Row conversion failed", groupTry.failure());
                    }
                    batch.add(groupTry.get());

                    if (batch.size() >= batchSize) {
                        for (Group g : batch) writer.write(g);
                        rowCount += batch.size();
                        log.info("Wrote {} rows", rowCount);
                        batch.clear();
                    }
                }
                if (!batch.isEmpty()) {
                    for (Group g : batch) writer.write(g);
                    rowCount += batch.size();
                }
            }

            log.info("Batched write complete. Total rows: {}", rowCount);
            // Return path without prefix for backward compatibility
            return new FileMetaData(fullPath, (int) rowCount, "");
        });
    }

}
