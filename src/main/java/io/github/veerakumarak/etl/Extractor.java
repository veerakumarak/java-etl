package io.github.veerakumarak.etl;

import io.github.veerakumarak.etl.entities.FileMetaData;
import io.github.veerakumarak.etl.entities.ExtractResult;
import io.github.veerakumarak.etl.datasource.IDataSource;
import io.github.veerakumarak.etl.file.GenericFileReader;
import io.github.veerakumarak.etl.parquet.ParquetWriterHelper;
import io.github.veerakumarak.etl.utils.TemplateUtil;
import io.github.veerakumarak.fp.Result;

import java.sql.*;
import java.util.Map;

public class Extractor {

    private Extractor() {
    }

    public static QueryProviderStep job(String jobName, IDataSource dataSource) {
        return new Builder(jobName, dataSource);
    }

    public interface QueryProviderStep {
        ParametersStep withQuery(String sqlPath);
    }

    public interface ParametersStep {
        FetchSizeStep withParameters(Map<String, String> parameters);
    }

    public interface FetchSizeStep {
        WriterStep withFetchSize(int fetchSize);
    }

    public interface WriterStep {
        Result<ExtractResult> toParquet(String writePath);
    }

    private static class Builder implements QueryProviderStep, ParametersStep, FetchSizeStep, WriterStep {
        private final String jobName;
        private final IDataSource dataSource;
        private String queryPath;
        private Map<String, String> parameters;
        private int fetchSize;
        private String writePath;

        public Builder(String jobName, IDataSource dataSource) {
            this.jobName = jobName;
            this.dataSource = dataSource;
        }

        @Override
        public ParametersStep withQuery(String queryPath) {
            this.queryPath = queryPath;
            return this;
        }

        @Override
        public FetchSizeStep withParameters(Map<String, String> parameters) {
            this.parameters = parameters;
            return this;
        }

        @Override
        public WriterStep withFetchSize(int fetchSize) {
            this.fetchSize = fetchSize;
            return this;
        }

        @Override
        public Result<ExtractResult> toParquet(String writePath) {
            this.writePath = writePath;
            return build();
        }

        private Result<ExtractResult> build() {
            return Result.of(() -> {
                String queryTemplate = GenericFileReader.readFile(queryPath)
                        .expect("unable to read SQL file");

                String query = TemplateUtil.render(queryTemplate, parameters);

                try (Connection conn = dataSource.connection().get();
                     PreparedStatement ps = conn.prepareStatement(query)) {

                    ps.setFetchSize(fetchSize);

                    try (ResultSet rs = ps.executeQuery()) {

                        FileMetaData fileMetaData = ParquetWriterHelper.toFile(writePath, jobName, rs).get();

                        return new ExtractResult(fileMetaData.filePath(), fileMetaData.count());
                    }
                }
            });
        }
    }

}