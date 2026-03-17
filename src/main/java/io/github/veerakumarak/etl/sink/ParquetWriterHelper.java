package io.github.veerakumarak.etl.sink;

import io.github.veerakumarak.etl.entities.FileMetaData;
import io.github.veerakumarak.etl.parquet.MessageTypeConverter;
import io.github.veerakumarak.etl.parquet.ParquetAwsManager;
import io.github.veerakumarak.etl.parquet.ResultSetToGroupConverter;
import io.github.veerakumarak.fp.Result;
import io.github.veerakumarak.fp.failures.InternalFailure;
import io.github.veerakumarak.fp.failures.InvalidRequest;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class ParquetWriterHelper {

    private static final Logger log = LoggerFactory.getLogger(ParquetWriterHelper.class);

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

    protected static Result<FileMetaData> writeBatched(String outputPath, String tableName, ResultSet resultSet, Integer batchSize) {

        return Result.of(() -> {
            if (Objects.isNull(resultSet)) {
                throw new InternalFailure("ResultSet is null");
            }
            if (batchSize == null || batchSize <= 0) {
                throw new InvalidRequest("Batch size must be positive");
            }

            ResultSetMetaData metaData = resultSet.getMetaData();
            MessageType schema = MessageTypeConverter.fromResultSet(tableName, metaData)
                    .orElseThrow(() -> new InternalFailure("Could not get schema from result set"));

            String filePath = outputPath + "/" + tableName + ".parquet";

            long rowCount = 0;
            try (ParquetWriter<Group> writer = createWriter(filePath, schema, ParquetAwsManager.getConfiguration())) {
                List<Group> batch = new ArrayList<>(batchSize);
                while (resultSet.next()) {
                    Group group = ResultSetToGroupConverter.convert(schema, metaData, resultSet)
                            .orElseThrow(() -> new InternalFailure("Could not get group from result set"));
                    batch.add(group);

                    if (batch.size() >= batchSize) {
                        for (Group g : batch) writer.write(g);
                        rowCount += batch.size();
                        batch.clear();
                    }
                }
                if (!batch.isEmpty()) {
                    for (Group g : batch) writer.write(g);
                    rowCount += batch.size();
                }
            }

            //ToDo partition prefixer is pending
            log.info("Batched write complete. Total rows: {}", rowCount);
            return new FileMetaData(filePath, (int) rowCount, "");
        });
    }

}
