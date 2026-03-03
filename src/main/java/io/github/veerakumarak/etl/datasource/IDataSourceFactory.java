package io.github.veerakumarak.etl.datasource;

import io.github.veerakumarak.fp.Result;

public interface IDataSourceFactory {
    Result<IDataSource> getDataSource(String dataSourceName);
}
