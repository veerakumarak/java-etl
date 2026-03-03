package io.github.veerakumarak.etl.parquet;

import io.github.veerakumarak.etl.datasource.IDataSource;
import io.github.veerakumarak.fp.Result;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ParquetReaderHelper {

    private static final Logger log = LoggerFactory.getLogger(ParquetReaderHelper.class);

    /**
     * Reads Parquet from the given path and inserts into database.
     */
    public static Result<Long> fromParquet(String filePath, IDataSource dataSource, String tableName,
                                           Integer batchSize, Configuration conf) {
        return readGroups(filePath, conf)
            .flatMap(groups -> insertGroups(groups, dataSource, tableName, batchSize));
    }

    private static Result<Long> insertGroups(List<Group> groups, IDataSource dataSource, String tableName, Integer batchSize) {
        if (groups.isEmpty()) {
            log.info("Parquet contains no records. Skipping writing to db");
            return Result.ok(0L);
        }
        
        MessageType schema = (MessageType) groups.getFirst().getType();
        return dataSource.connection()
            .flatMap(connection -> {
                Result<Long> result = DataBaseHelper.insertBatch(dataSource, connection, groups, tableName, schema, batchSize);
//                Result.run(connection::close);
                try {
                    connection.close();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
                return result;
            });
    }

    private static Result<List<Group>> readGroups(String filePath, Configuration conf) {
        // Add s3a:// prefix for Hadoop to read from S3
        String s3Path = "s3a://" + filePath;
        try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), new Path(s3Path))
                .withConf(conf)
                .build()) {

            List<Group> groups = new ArrayList<>();
            Group group;
            while ((group = reader.read()) != null) {
                groups.add(group);
            }

            return Result.ok(groups);
        } catch (IOException e) {
            return Result.failure(e.getMessage());
        }
    }

}