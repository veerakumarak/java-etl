package io.github.veerakumarak.etl.file;

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
            cleanPath = cleanPath.startsWith("/") ? cleanPath.substring(1) : cleanPath;
            try (InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(cleanPath)) {
                if (is == null) throw new InvalidRequest("Classpath resource not found: " + cleanPath);
                return new String(is.readAllBytes(), StandardCharsets.UTF_8);
//                return new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))
//                        .lines().collect(Collectors.joining("\n"));
            }
        });
    }
}