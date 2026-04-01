package io.github.veerakumarak.etl.parquet.partitions;

import io.github.veerakumarak.fp.Result;
import io.github.veerakumarak.fp.failures.InternalFailure;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;

public class ParquetPartitionHelper {

    private static final Logger log = LoggerFactory.getLogger(ParquetPartitionHelper.class);

    public static Result<String> getPartitionPrefix(MessageType schema, List<String> partitionKeys, Group group) {
        return Result.of(() -> {
            List<String> segments = partitionKeys.stream()
                    .map(fieldName -> {
                        if (!schema.containsField(fieldName)) {
                            throw new InternalFailure("partition key '" + fieldName + "' not found in schema '" + schema.getName() + "'");
                        }

                        int fieldIndex = schema.getFieldIndex(fieldName);
                        if (fieldIndex < 0 || group.getFieldRepetitionCount(fieldIndex) == 0) {
                            throw new InternalFailure("partition key '" + fieldName + "' is not present in this group");
                        }

                        String extractedValue = group.getValueToString(fieldIndex, 0);
                        if (Objects.isNull(extractedValue) || extractedValue.isEmpty()) {
                            throw new InternalFailure("partition key '" + fieldName + "' has null or empty value");
                        }
                        return fieldName + "=" + extractedValue;
                    })
                    .toList();

            return String.join("/", segments);
        });
    }
}