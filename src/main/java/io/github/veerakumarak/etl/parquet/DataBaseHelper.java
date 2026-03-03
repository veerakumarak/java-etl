package io.github.veerakumarak.etl.parquet;

import io.github.veerakumarak.etl.datasource.IDataSource;
import io.github.veerakumarak.fp.Pair;
import io.github.veerakumarak.fp.Result;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Types;
import java.util.List;

public class DataBaseHelper {

    private static final Logger log = LoggerFactory.getLogger(DataBaseHelper.class);

    private static final int DEFAULT_BATCH_SIZE = 100;

    private static Integer getSqlType(Type field) {
        LogicalTypeAnnotation logicalType = field.getLogicalTypeAnnotation();

        if (logicalType != null) {
            if (logicalType.equals(LogicalTypeAnnotation.stringType())) {
                return Types.VARCHAR;
            }
            else if (logicalType.equals(LogicalTypeAnnotation.dateType())) {
                return Types.DATE;
            }
            else if (logicalType.equals(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS))) {
                return Types.TIMESTAMP;
            }
            else if (logicalType.equals(LogicalTypeAnnotation.timeType(true, LogicalTypeAnnotation.TimeUnit.MILLIS))) {
                return Types.TIME;
            }
            else {
                log.warn("Unhandled logical type {} for field {}. Defaulting to VARCHAR", logicalType, field.getName());
                return Types.VARCHAR;
            }
        } else if (field.isPrimitive()) {
            PrimitiveType primitiveType = field.asPrimitiveType();
            switch (primitiveType.getPrimitiveTypeName()) {
                case INT32:
                    return Types.INTEGER;
                case INT64:
                    return Types.BIGINT;
                case DOUBLE:
                    return Types.DOUBLE;
                case FLOAT:
                    return Types.FLOAT;
                case BOOLEAN:
                    return Types.BOOLEAN;
                case BINARY:
                    return Types.VARBINARY;
                default:
                    log.warn("Unhandled primitive type for field {}. Defaulting to VARCHAR", field.getName());
                    return Types.VARCHAR;
            }
        } else {
            log.warn("Unhandled config type for field {}. Defaulting to VARCHAR", field.getName());
            return Types.VARCHAR;
        }
    }

    private static Object getValueOrNull(Group group, String fieldName, int sqlType){
        if(group.getFieldRepetitionCount(fieldName)==0){
            return null;
        }

        switch(sqlType){
            case Types.DATE:
                int daysFromEpoch = group.getInteger(fieldName, 0);
                return java.sql.Date.valueOf(java.time.LocalDate.ofEpochDay(daysFromEpoch));

            case Types.TIMESTAMP:
                long epochMillis = group.getLong(fieldName, 0);
                return java.sql.Timestamp.from(java.time.Instant.ofEpochMilli(epochMillis));

            case Types.TIME:
                int millisOfDay = group.getInteger(fieldName, 0);
                return java.sql.Time.valueOf(java.time.LocalTime.ofNanoOfDay(millisOfDay * 1_000_000L));

            case Types.VARCHAR:
                return group.getString(fieldName, 0);

            case Types.INTEGER:
                return group.getInteger(fieldName, 0);

            case Types.BIGINT:
                return group.getLong(fieldName, 0);

            case Types.DOUBLE:
                return group.getDouble(fieldName, 0);

            case Types.FLOAT:
                return group.getFloat(fieldName, 0);

            case Types.BOOLEAN:
                return group.getBoolean(fieldName, 0);

            case Types.VARBINARY:
                return group.getBinary(fieldName, 0).getBytes();

            default:
                log.error("Unhandled sqlType {}. Returning null", sqlType);
                return null;
        }
    }

    private static Pair<Object, Integer> extractFieldValue(Group group, Type field, MessageType schema) {
        try {
            int sqlType = getSqlType(field);
            Object value = getValueOrNull(group, field.getName(), sqlType);
            return Pair.of(value, sqlType);
        } catch (Exception e) {
            log.error("Unexpected error '{}' when converting field {}. Inserting null String", e.getMessage(), field.getName());
            return Pair.of(null, Types.VARCHAR);
        }
    }

    public static Result<Long> insertBatch(IDataSource dataSource, Connection connection, List<Group> groups, String tableName, MessageType schema, Integer batchSize) {
        if (groups.isEmpty()) return Result.ok(0L);

        List<Type> fields = schema.getFields();
        List<String> columnNames = fields.stream().map(Type::getName).toList();
        String sql = buildInsertSql(columnNames, tableName);

        List<List<Pair<Object, Integer>>> batchValuesAndTypes = groups.stream()
                .map(group -> fields.stream()
                        .map(field -> extractFieldValue(group, field, schema))
                        .toList())
                .toList();

        Integer finalBatchSize = batchSize != null? batchSize: DEFAULT_BATCH_SIZE;
        return dataSource.insertBatch(connection, sql, batchValuesAndTypes, finalBatchSize);
    }

    private static String buildInsertSql(List<String> columnNames, String tableName) {
        String columns = String.join(", ", columnNames);
        String marks = String.join(", ", columnNames.stream().map(c -> "?").toList());
        return "INSERT INTO " + tableName + " (" + columns + ") VALUES (" + marks + ")";
    }
}
