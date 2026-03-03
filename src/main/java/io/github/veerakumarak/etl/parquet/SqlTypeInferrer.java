package io.github.veerakumarak.etl.parquet;

import io.github.veerakumarak.fp.Result;

import java.sql.ResultSetMetaData;
import java.util.Locale;

public class SqlTypeInferrer {

    public static Result<Integer> getEffectiveType(ResultSetMetaData metaData, int columnIndex) {
        return Result.of(() -> {
            int originalType = metaData.getColumnType(columnIndex);

            if (originalType != java.sql.Types.OTHER) {
                return originalType;
            }

            // Logic to infer type from name only when necessary
            String typeName = metaData.getColumnTypeName(columnIndex).toUpperCase(Locale.ROOT);

            if (typeName.contains("TIMESTAMP") || typeName.contains("DATETIME")) {
                return java.sql.Types.TIMESTAMP;
            } else if (typeName.contains("DATE") && !typeName.contains("TIME")) {
                return java.sql.Types.DATE;
            } else if (typeName.contains("NUMERIC") || typeName.contains("DECIMAL") || typeName.contains("NUMBER")) {
                return java.sql.Types.DECIMAL;
            } else if (typeName.contains("BIGINT")) {
                return java.sql.Types.BIGINT;
            } else if (typeName.contains("INT") || typeName.contains("INTEGER")) {
                return java.sql.Types.INTEGER;
            } else if (typeName.contains("BOOL")) {
                return java.sql.Types.BOOLEAN;
            }

            // Safe fallback for complex types (JSON, UUID, GEOMETRY)
            return java.sql.Types.VARCHAR;
        });
    }
}