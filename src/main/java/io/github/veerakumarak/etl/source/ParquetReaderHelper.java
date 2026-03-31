package io.github.veerakumarak.etl.source;

import io.github.veerakumarak.etl.parquet.ClassHelper;
import io.github.veerakumarak.etl.parquet.converters.GroupToClassConverter;
import io.github.veerakumarak.etl.parquet.ParquetAwsManager;
import io.github.veerakumarak.etl.parquet.data.DataAnnotationHelper;
import io.github.veerakumarak.fp.Result;
import io.github.veerakumarak.fp.failures.EntityNotFound;
import io.github.veerakumarak.fp.failures.InternalFailure;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class ParquetReaderHelper {

    private static final Logger log = LoggerFactory.getLogger(ParquetReaderHelper.class);

    public static Result<MessageType> readSchema(String filePath, Configuration conf) {
        return Result.of(() -> {
            try (ParquetFileReader fileReader = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(filePath), conf))) {
                FileMetaData metaData = fileReader.getFileMetaData();
                return metaData.getSchema();
            }
        });
    }

    public static ParquetReader<Group> createReader(String filePath, Configuration conf) throws IOException {
        return ParquetReader.builder(new GroupReadSupport(), new Path(filePath))
                .withConf(conf)
                .build();
    }

    private static void validateFields(Field[] classFields, List<Type> parquetFields, Class<?> tClass, boolean relaxedValidation) {
        Set<String> fieldNames = parquetFields.stream().map(Type::getName).collect(Collectors.toSet());

        if (relaxedValidation) {
            // Relaxed mode: Allow schema mismatch, missing fields will be null
            for (Field field : classFields) {
                String parameterName = DataAnnotationHelper.getName(field);
                if (!fieldNames.contains(parameterName)) {
                    log.warn("Field '{}' in class '{}' not found in Parquet schema, will be set to null",
                            parameterName, tClass.getSimpleName());
                }
            }
        } else {
            // Strict mode (default): field count must match and all fields must exist
            if (classFields.length != parquetFields.size()) {
                throw new IllegalArgumentException("Number of field values does not match constructor parameters.");
            }
            for (Field field : classFields) {
                String parameterName = DataAnnotationHelper.getName(field);
                if (!fieldNames.contains(parameterName)) {
                    throw new IllegalArgumentException("Parameter not found in fields " + parameterName);
                }
            }
        }
    }

/*    private record ReaderContext<T>(
            ParquetReader<Group> reader,
            Constructor<?> constructor,
            Field[] classFields,
            Map<String, Type> fieldsMap
    ) {
    }

    private static <T> Result<ReaderContext<T>> prepareReaderContext(String filePath, Class<T> tClass, boolean relaxedValidation) {
        return Result.of(() -> {
            Configuration configuration = ParquetAwsManager.getConfiguration();
            MessageType schema = readSchema(filePath, configuration).orElseThrow();

            List<Type> parquetFields = schema.getFields();
            Field[] classFields = tClass.getDeclaredFields();
            Constructor<?> constructor = ClassHelper.getAllArgsConstructor(tClass)
                    .orElseThrow(() -> new InternalFailure("AllArgsConstructor missing: " + tClass.getName()));

            Map<String, Type> fieldsMap = parquetFields.stream()
                    .collect(Collectors.toMap(Type::getName, field -> field));

            validateFields(classFields, parquetFields, tClass, relaxedValidation);

            ParquetReader<Group> reader = createReader(filePath, configuration);
            return new ReaderContext<>(reader, constructor, classFields, fieldsMap);
        });
    }
*/
    public static <T> Result<Stream<T>> readStream(String filePath, Class<T> tClass) {
        return Result.of(() -> {
            Configuration configuration = ParquetAwsManager.getConfiguration();

            MessageType schema = readSchema(filePath, configuration).orElseThrow();
            List<Type> parquetFields = schema.getFields();
            Map<String, Type> fieldsMap = parquetFields.stream()
                    .collect(Collectors.toMap(Type::getName, field -> field));

            Field[] classFields = tClass.getDeclaredFields();
            Constructor<?> constructor = ClassHelper.getAllArgsConstructor(tClass)
                    .orElseThrow(() -> new InternalFailure("AllArgsConstructor missing: " + tClass.getName()));

            validateFields(classFields, parquetFields, tClass, false);
            return readStream(filePath).orElseThrow().map(group -> {
                try {
                    Object[] values = GroupToClassConverter.convert(group, classFields, fieldsMap, false);
                    return (T) constructor.newInstance(values);
                } catch (Exception e) {
                    throw new InternalFailure("Parquet stream read failed: " + e);
                }
            });
        });
    }

    public static <T> Result<List<T>> readList(String filePath, Class<T> tClass) {
        return readStream(filePath, tClass).map(stream -> {
            try (stream) {
                return stream.toList();
            }
        });
    }

    public static Result<Stream<Group>> readStream(String filePath) {
        return Result.of(() -> {
            Configuration configuration = ParquetAwsManager.getConfiguration();
            ParquetReader<Group> reader = createReader(filePath, configuration);

            Iterator<Group> iterator = new Iterator<>() {
                private Group nextRecord;

                @Override
                public boolean hasNext() {
                    try {
                        if (nextRecord == null) {
                            nextRecord = reader.read();
                        }
                        return nextRecord != null;
                    } catch (Exception e) {
                        throw new InternalFailure("Parquet stream read failed: " + e);
                    }
                }

                @Override
                public Group next() {
                    if (!hasNext()) throw new EntityNotFound("No more records to read");
                    Group res = nextRecord;
                    nextRecord = null;
                    return res;
                }
            };

            return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, 0), false)
                    .onClose(() -> {
                        try {
                            reader.close();
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });

        });
    }

    public static Result<List<Group>> readList(String filePath) {
        return readStream(filePath).map(stream -> {
            try (stream) {
                return stream.toList();
            }
        });
    }


}