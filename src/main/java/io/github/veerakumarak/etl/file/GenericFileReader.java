package io.github.veerakumarak.etl.file;

import io.github.veerakumarak.fp.Result;

import java.util.List;

public class GenericFileReader {
    private static final List<FileLoader> loaders = List.of(
//            new S3FileLoader(),
            new LocalFileLoader(),
            new ClassFileLoader()
    );

    public static Result<String> readFile(String path) {
        return loaders.stream()
                .filter(loader -> loader.canHandle(path))
                .findFirst()
                .map(loader -> loader.read(path))
                .orElse(Result.failure("can not read the source file"));
    }

//    public static void main(String[] args) {
//        String path = "file:///Users/veerakumar/opensource/java-etl/src/main/resources/extract/sybase_primary/station_master_extract/dma.sql";
//        System.out.println(readFile(path));
//        path = "classpath:extract/sybase_primary/station_master_extract/dma.sql";
//        System.out.println(readFile(path));
//    }
}