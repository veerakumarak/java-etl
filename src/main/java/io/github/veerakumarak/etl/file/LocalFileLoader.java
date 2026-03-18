package io.github.veerakumarak.etl.file;

import io.github.veerakumarak.etl.utils.Mapper;
import io.github.veerakumarak.fp.Result;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

public class LocalFileLoader implements FileLoader {
    @Override
    public boolean canHandle(String path) {
        return path.startsWith("file://");
    }

    @Override
    public Result<String> read(String path){
        return Result.of(() -> {
            String cleanPath = path.replace("file://", "");
            return Files.readString(Path.of(cleanPath), StandardCharsets.UTF_8);
        });
    }

    public <T> Result<T> load(String path, Class<T> clazz) {
        return read(path)
                .flatMap(content -> Mapper.deserialize(content, clazz));
    }
}