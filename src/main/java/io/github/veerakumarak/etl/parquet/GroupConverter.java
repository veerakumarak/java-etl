package io.github.veerakumarak.etl.parquet;

import io.github.veerakumarak.fp.Result;
import io.github.veerakumarak.fp.failures.InternalFailure;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.*;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.List;

public class GroupConverter {

    private static final Logger log = LoggerFactory.getLogger(GroupConverter.class);

    public static Result<List<Group>> convert(ResultSetMetaData sourceMetaData, ResultSetMetaData targetMetaData, MessageType schema, ResultSet rs) {
        return Result.of(() -> {
            List<Group> groups = new ArrayList<>();
            while (rs.next()) {
                Result<Group> groupResult = convert(schema, sourceMetaData, targetMetaData, rs);
                if (groupResult.isFailure()) {
                    log.error(groupResult.failure().getMessage());
                    throw new InternalFailure("Unable to convert ResultSet to Group: " + groupResult.failure().getMessage());
                }
                groups.add(groupResult.get());
            }
            return groups;
        });
    }

    /**
     * Converts a single row from ResultSet to a Parquet Group.
     * This is a public wrapper for the private convert method, used for batched processing.
     */
    public static Result<Group> convertSingleRow(MessageType schema, ResultSetMetaData sourceMetaData, ResultSetMetaData targetMetaData, ResultSet rs) {
        return convert(schema, sourceMetaData, targetMetaData, rs);
    }

    private static Result<Group> convert(MessageType schema, ResultSetMetaData sourceMetaData, ResultSetMetaData targetMetaData, ResultSet rs) {
        return Result.of(() -> {
            Group group = new SimpleGroup(schema);
            int columnCount = targetMetaData.getColumnCount();
            for (int i = 1; i <= columnCount; i++) {
                // Use getColumnLabel() to get the alias (AS name) from SELECT queries
                // This ensures we use the alias (e.g., "PrgsvcID") instead of original column name (e.g., "prgsvcid")
                String columnName = targetMetaData.getColumnLabel(i);
                int columnType = SqlTypeInferrer.getEffectiveType(targetMetaData, i).orElseThrow();
                int scale = targetMetaData.getScale(i);
                int precision = targetMetaData.getPrecision(i);

                switch (columnType) {
                    case Types.BOOLEAN:
                    case Types.BIT:
                        boolean booleanValue = rs.getBoolean(columnName);
                        if (!rs.wasNull()) {
                            group.add(columnName, booleanValue);
                        }
                        break;
                    case Types.BIGINT:
                        long longValue = rs.getLong(columnName);
                        if (!rs.wasNull()) {
                            group.add(columnName, longValue);
                        }
                        break;
                    case Types.INTEGER:
                    case Types.SMALLINT:
                    case Types.TINYINT:
                        int intValue = rs.getInt(columnName);
                        if (!rs.wasNull()) {
                            group.add(columnName, intValue);
                        }
                        break;
                    case Types.FLOAT:
                    case Types.REAL:
                        float floatValue = rs.getFloat(columnName);
                        if (!rs.wasNull()) {
                            group.add(columnName, floatValue);
                        }
                        break;
                    case Types.DOUBLE:
                        double doubleValue = rs.getDouble(columnName);
                        if (!rs.wasNull()) {
                            group.add(columnName, doubleValue);
                        }
                        break;
                    case Types.VARCHAR:
                    case Types.NVARCHAR:
                    case Types.LONGVARCHAR:
                    case Types.LONGNVARCHAR:
                    case Types.CHAR:
                    case Types.NCHAR:
                    case Types.CLOB:
                    case Types.NCLOB:
                        String s = rs.getString(columnName);
                        if (!rs.wasNull()) {
                            group.add(columnName, s);
                        }
                        break;
                    case Types.DATE:
                        Date d = rs.getDate(columnName);
                        if (!rs.wasNull()) {
                            group.add(columnName, (int) d.toLocalDate().toEpochDay());
                        }
                        break;
                    case Types.TIME:
                        Time t = rs.getTime(columnName);
                        if (!rs.wasNull()) {
                            LocalTime localTime = t.toLocalTime();
                            long milliOfDay = localTime.get(ChronoField.MILLI_OF_DAY);
                            group.add(columnName, (int) milliOfDay);
                            // group.add(columnName, t.getTime());
                        }
                        break;
                    case Types.TIMESTAMP:
                        Timestamp ts = rs.getTimestamp(columnName);

                        if (ts != null) {
                            LocalDateTime ldt = ts.toLocalDateTime();
                            Instant instant = ldt.toInstant(ZoneOffset.UTC);
                            long epochMillis = instant.toEpochMilli();
                            group.add(columnName, epochMillis);
                        }
                        break;
                    case Types.DECIMAL:
                    case Types.NUMERIC:
                        BigDecimal decimal = rs.getBigDecimal(columnName);
                        if (!rs.wasNull()) {
                            if (scale > 0 && precision <= 18) {
                                group.add(columnName, decimal.unscaledValue().longValue());
                            } else if (scale > 0) {
                                byte[] bytes = decimal.unscaledValue().toByteArray();
                                group.add(columnName, Binary.fromConstantByteArray(bytes));
                            } else if (precision < 5) {
                                group.add(columnName, decimal.intValue());
                            } else {
                                group.add(columnName, decimal.longValue());
                            }
                        }
                        break;
                    case Types.VARBINARY:
                        byte[] bytesValue = rs.getBytes(columnName);
                        if (!rs.wasNull()) {
                            // Use Parquet's Binary class
                            group.add(columnName, Binary.fromConstantByteArray(bytesValue));
                        }
                        break;
                    default: {
                        log.info("Unsupported SQL type: {}", columnType);
                        throw new IllegalArgumentException("Unsupported SQL type: " + columnType);
                    }
                }
            }
            return group;
        });

    }
}
