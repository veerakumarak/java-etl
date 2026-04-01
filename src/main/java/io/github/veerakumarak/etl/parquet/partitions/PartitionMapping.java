package io.github.veerakumarak.etl.parquet.partitions;

import org.apache.parquet.example.data.Group;

import java.util.function.Function;

public record PartitionMapping<T>(
        String keyName,
        Function<Group, T> extractor
) {}