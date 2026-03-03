package io.github.veerakumarak.etl.entities;

public record FileMetaData(
        String filePath,
        Integer count,
        String partitionPrefix
) {
}
