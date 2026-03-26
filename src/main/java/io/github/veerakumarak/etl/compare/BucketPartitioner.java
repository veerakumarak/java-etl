package io.github.veerakumarak.etl.compare;

import io.github.veerakumarak.etl.source.ParquetReaderHelper;
import io.github.veerakumarak.fp.Result;
import io.github.veerakumarak.fp.failures.InternalFailure;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Stream;

public class BucketPartitioner {

    protected static final Integer NO_OF_BUCKETS = 64;
    protected static final String TEMP_DIR = "/tmp/buckets";

    public static Result<Map<Integer, List<String>>> partition(String sourcePath, String prefix, Function<Group, Integer> hashFunction) {
        return Result.of(() -> {
            Map<Integer, List<String>> bucketMap = new HashMap<>();

            Map<Integer, List<Group>> buffers = new HashMap<>();

            try (Stream<Group> stream = ParquetReaderHelper.readStream(sourcePath).orElseThrow()) {
                stream.forEach(row -> {
                    int bucketId = Math.abs(hashFunction.apply(row)) % NO_OF_BUCKETS;
                    buffers.computeIfAbsent(bucketId, k -> new ArrayList<>()).add(row);

                    // Flush buffer to disk if it gets too big (e.g., 1000 rows)
                    if (buffers.get(bucketId).size() >= 1000) {
                        bucketMap.computeIfAbsent(bucketId, k -> new ArrayList<>());
                        String path = flushToBucket(prefix, bucketId, buffers.get(bucketId), bucketMap.get(bucketId).size()).orElseThrow();
                        bucketMap.get(bucketId).add(path);
                        buffers.get(bucketId).clear();
                    }
                });
            }

            // Final flush for remaining rows
            buffers.forEach((id, list) -> {
                bucketMap.computeIfAbsent(id, k -> new ArrayList<>());
                String path = flushToBucket(prefix, id, list, bucketMap.get(id).size()).orElseThrow();
                bucketMap.get(id).add(path);
            });
            return bucketMap;
        });
    }

    public static <T> Result<Map<Integer, List<String>>> partition2(String sourcePath, String prefix, Function<Group, Integer> hashFunction) {
        return Result.of(() -> {
            Map<Integer, List<String>> bucketMap = new HashMap<>();

            Map<Integer, List<Group>> buffers = new HashMap<>();

            try (Stream<Group> stream = ParquetReaderHelper.readStream(sourcePath).orElseThrow()) {
                stream.forEach(row -> {
                    int bucketId = Math.abs(hashFunction.apply(row)) % NO_OF_BUCKETS;
                    buffers.computeIfAbsent(bucketId, k -> new ArrayList<>()).add(row);

                    // Flush buffer to disk if it gets too big (e.g., 1000 rows)
                    if (buffers.get(bucketId).size() >= 1000) {
                        bucketMap.computeIfAbsent(bucketId, k -> new ArrayList<>());
                        String path = flushToBucket(prefix, bucketId, buffers.get(bucketId), bucketMap.get(bucketId).size()).orElseThrow();
                        bucketMap.get(bucketId).add(path);
                        buffers.get(bucketId).clear();
                    }
                });
            }

            // Final flush for remaining rows
            buffers.forEach((id, list) -> {
                bucketMap.computeIfAbsent(id, k -> new ArrayList<>());
                String path = flushToBucket(prefix, id, list, bucketMap.get(id).size()).orElseThrow();
                bucketMap.get(id).add(path);
            });
            return bucketMap;
        });
    }

    private static Result<String> flushToBucket(String prefix, int id, List<Group> data, int partId) {
        return Result.of(() -> {
            if (data.isEmpty()) throw new InternalFailure("Empty data list for bucket " + id);
            String path = String.format("%s/%s/bucket_%s_part_%s.parquet", TEMP_DIR, prefix, id, partId);
            org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(path);
            Configuration conf = new Configuration();
            GroupType schema = data.get(0).getType();

            try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(hadoopPath)
                    .withType((MessageType) schema)
                    .withConf(conf)
                    .withWriteMode(ParquetFileWriter.Mode.CREATE)
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
//                    .withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
//                    .withDictionaryEncoding(true)
                    .build()) {

                for (Group group : data) {
                    writer.write(group);
                }
                return path;

            } catch (IOException e) {
                throw new InternalFailure("Error writing to bucket " + id + " at " + path + ", " + e);
            }
        });
    }
}