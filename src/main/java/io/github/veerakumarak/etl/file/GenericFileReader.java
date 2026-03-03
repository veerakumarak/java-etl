package io.github.veerakumarak.etl.file;

import io.github.veerakumarak.fp.Result;

import java.util.List;

public class GenericFileReader {
    private static final List<FileLoader> loaders = List.of(
            new S3FileLoader(),
            new LocalFileLoader()
    );

    public static Result<String> readFile(String path) {
        return loaders.stream()
                .filter(loader -> loader.canHandle(path))
                .findFirst()
                .map(loader -> loader.read(path))
                .orElse(Result.ok(path));
    }
}