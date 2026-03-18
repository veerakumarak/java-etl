package io.github.veerakumarak.etl.file;

import io.github.veerakumarak.etl.utils.Mapper;
import io.github.veerakumarak.fp.Result;
import io.github.veerakumarak.fp.failures.InvalidRequest;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class ClassFileLoader implements FileLoader {

    private static final String PREFIX = "classpath:";

    @Override
    public boolean canHandle(String path) {
        return path.startsWith(PREFIX);
    }

    @Override
    public Result<String> read(String path){
        return Result.of(() -> {
            String cleanPath = path.replace(PREFIX, "");
            if (cleanPath.startsWith("/")) {
                cleanPath = cleanPath.substring(1);
            }
            try (InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(cleanPath)) {
                if (is == null) throw new InvalidRequest("Classpath resource not found: " + cleanPath);
                return new String(is.readAllBytes(), StandardCharsets.UTF_8);
//                return new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))
//                        .lines().collect(Collectors.joining("\n"));
            }
        });
    }

    @Override
    public <T> Result<T> load(String path, Class<T> clazz) {
        return read(path)
                .flatMap(content -> Mapper.deserialize(content, clazz));
    }
}