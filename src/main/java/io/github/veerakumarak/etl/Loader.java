package io.github.veerakumarak.etl;

import io.github.veerakumarak.etl.datasource.IDataSource;
import io.github.veerakumarak.etl.entities.LoadResult;
import io.github.veerakumarak.etl.parquet.ParquetAwsManager;
import io.github.veerakumarak.etl.parquet.ParquetReaderHelper;
import io.github.veerakumarak.etl.utils.RegexUtil;
import io.github.veerakumarak.fp.Result;
import io.github.veerakumarak.fp.failures.InvalidRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;

public class Loader {

    private static final Logger log = LoggerFactory.getLogger(Loader.class);

    public static ScriptProviderStep job(String jobName, IDataSource dataSource) {
        return new Builder(jobName, dataSource);
    }

    public interface ScriptProviderStep {
        TableNameStep withScript(String scriptPath);
    }

    public interface TableNameStep {
        TruncateStep withTableName(String tableName);
    }

    public interface TruncateStep {
        BatchSizeStep truncate(Boolean truncate);
    }

    public interface BatchSizeStep {
        WriterStep withBatchSize(int batchSize);
    }

    public interface WriterStep {
        Result<LoadResult> fromParquet(String writePath);
    }

    private static class Builder implements ScriptProviderStep, TableNameStep, TruncateStep, BatchSizeStep, WriterStep {
        private final String jobName;
        private final IDataSource dataSource;
        private String scriptPath;
        private String tableName;
        private boolean truncate;
        private int batchSize;
        private String readPath;

        public Builder(String jobName, IDataSource dataSource) {
            this.jobName = jobName;
            this.dataSource = dataSource;
        }

        @Override
        public TableNameStep withScript(String scriptPath) {
            this.scriptPath = scriptPath;
            return this;
        }

        @Override
        public TruncateStep withTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        @Override
        public BatchSizeStep truncate(Boolean truncate) {
            this.truncate = truncate;
            return this;
        }

        @Override
        public WriterStep withBatchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        @Override
        public Result<LoadResult> fromParquet(String readPath) {
            this.readPath = readPath;
            return build();
        }

        private Result<LoadResult> build() {
            return Result.of(() -> {
                if (RegexUtil.matches("^[a-zA-Z_][a-zA-Z0-9_$]*$", tableName)) {
                    throw new InvalidRequest("Invalid Table Name: " + tableName);
                }

                if (this.truncate) {
                    try (Connection conn = dataSource.connection().get()) {
                        log.info("Truncating table: {} before inserting.", this.tableName);
                        dataSource.truncate(conn, this.tableName).orElseThrow();
                    }
                }
                long count = ParquetReaderHelper.fromParquet(readPath, dataSource, tableName, batchSize,
                                ParquetAwsManager.getConfiguration())
                        .orElseThrow();

                return new LoadResult(count);
            });
        }

    }

}