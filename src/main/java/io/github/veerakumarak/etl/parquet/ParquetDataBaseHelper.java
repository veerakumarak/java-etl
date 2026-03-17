package io.github.veerakumarak.etl.parquet;

import io.github.veerakumarak.etl.source.ParquetReaderHelper;
import io.github.veerakumarak.fp.Failure;
import io.github.veerakumarak.fp.Result;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.List;

public class ParquetDataBaseHelper {

    private static final Logger log = LoggerFactory.getLogger(ParquetDataBaseHelper.class);

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

    private static String buildInsertSql(List<String> columnNames, String tableName) {
        String columns = String.join(", ", columnNames);
        String marks = String.join(", ", columnNames.stream().map(c -> "?").toList());
        return "INSERT INTO " + tableName + " (" + columns + ") VALUES (" + marks + ")";
    }

    private record FieldMeta(
            String name,
            Type f,
            Integer sqlType
    ){}
    public static Result<Long> writeBatched(String filePath, Connection connection, String tableName, Integer batchSize) {
        return Result.of(() -> {
            Configuration conf = ParquetAwsManager.getConfiguration();

            // 1. Get Schema from Footer (Fast, no data read)
            MessageType schema = ParquetReaderHelper.readSchema(filePath, conf).orElseThrow();

            List<FieldMeta> fieldMetas = schema.getFields().stream()
                    .map(f -> new FieldMeta(f.getName(), f, getSqlType(f)))
                    .toList();

            String sql = buildInsertSql(fieldMetas.stream().map(FieldMeta::name).toList(), tableName);

            // 2. Open Reader and Statement
            try (ParquetReader<Group> reader = ParquetReaderHelper.createReader(filePath, conf);
                 PreparedStatement pstmt = connection.prepareStatement(sql)) {

                long count = 0;
                Group group;

                // 3. Stream through the file
                while ((group = reader.read()) != null) {
                    writeOne(pstmt, group, fieldMetas).orThrow();
                    count++;

                    // Execute batch based on user-defined size
                    if (count % batchSize == 0) {
                        pstmt.executeBatch();
                        log.info("Batch executed at {} records for table: {}", count, tableName);
                    }
                }

                // 4. Final partial batch flush
                if (count % batchSize != 0) {
                    pstmt.executeBatch();
                    log.info("Final batch executed. Total records: {}", count);
                }

                return count;
            }
        });
    }

    private static Failure writeOne(PreparedStatement pstmt, Group group, List<FieldMeta> metas) {
        return Failure.of(() -> {
            for (int i = 0; i < metas.size(); i++) {
                FieldMeta meta = metas.get(i);
                Object value = getValueOrNull(group, meta.name(), meta.sqlType());
                pstmt.setObject(i + 1, value, meta.sqlType());
            }
            pstmt.addBatch();
        });
    }

}
