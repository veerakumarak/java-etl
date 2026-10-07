package io.github.veerakumarak.etl.parquet;

import io.github.veerakumarak.etl.source.ParquetReaderHelper;
import io.github.veerakumarak.etl.utils.DateUtil;
import io.github.veerakumarak.fp.Failure;
import io.github.veerakumarak.fp.Result;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.NanoTime;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.ZoneId;
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
            else if (logicalType instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) {
                return Types.TIMESTAMP;
            }
            else if (logicalType instanceof LogicalTypeAnnotation.TimeLogicalTypeAnnotation) {
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
                case INT96:
                    // INT96 has no logical type annotation but always represents a timestamp
                    // (legacy format written by older engines such as Spark < 3.0).
                    return Types.TIMESTAMP;
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

    private static Object getValueOrNull(Group group, String fieldName, int sqlType, Type field, ZoneId zoneId){
        if(group.getFieldRepetitionCount(fieldName)==0){
            return null;
        }

        switch(sqlType){
            case Types.DATE:
                int daysFromEpoch = group.getInteger(fieldName, 0);
                return java.sql.Date.valueOf(java.time.LocalDate.ofEpochDay(daysFromEpoch));

            case Types.TIMESTAMP:
                // Legacy INT96 timestamps (e.g. older Spark) encode Julian day + nanos-of-day as a
                // zone-less wall-clock value; interpret it in the configured zone to build the instant.
                if (field.isPrimitive()
                        && field.asPrimitiveType().getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.INT96) {
                    NanoTime nanoTime = NanoTime.fromBinary(group.getInt96(fieldName, 0));
                    return java.sql.Timestamp.from(
                            DateUtil.int96ToInstant(nanoTime.getJulianDay(), nanoTime.getTimeOfDayNanos(), zoneId));
                }
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
    /**
     * Backward-compatible overload that decodes timestamp columns using the JVM default time zone.
     *
     * @see #writeBatched(String, Connection, String, Integer, ZoneId)
     */
    public static Result<Long> writeBatched(String filePath, Connection connection, String tableName, Integer batchSize) {
        return writeBatched(filePath, connection, tableName, batchSize, ZoneId.systemDefault());
    }

    /**
     * Streams a Parquet file into a database table in batches.
     *
     * @param zoneId time zone used to interpret timestamp columns. INT64 timestamps are epoch instants
     *               (zone-independent); legacy INT96 timestamps are zone-less wall-clock values and are
     *               interpreted in this zone when building {@code java.sql.Timestamp} values.
     */
    public static Result<Long> writeBatched(String filePath, Connection connection, String tableName, Integer batchSize, ZoneId zoneId) {
        ZoneId zone = zoneId != null ? zoneId : ZoneId.systemDefault();
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
                    writeOne(pstmt, group, fieldMetas, zone).orThrow();
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

    private static Failure writeOne(PreparedStatement pstmt, Group group, List<FieldMeta> metas, ZoneId zoneId) {
        return Failure.of(() -> {
            for (int i = 0; i < metas.size(); i++) {
                FieldMeta meta = metas.get(i);
                Object value = getValueOrNull(group, meta.name(), meta.sqlType(), meta.f(), zoneId);
                pstmt.setObject(i + 1, value, meta.sqlType());
            }
            pstmt.addBatch();
        });
    }

}
