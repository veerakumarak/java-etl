package io.github.veerakumarak.etl.utils;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.veerakumarak.fp.Result;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;

public class Mapper {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    static {
        OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    public static <T> Result<T> deserialize(InputStream body, Class<T> clazz) {
        return Result.of(() -> OBJECT_MAPPER.readValue(body, clazz));
    }

    public static <T> Result<T> deserialize(File file, Class<T> clazz) {
        return Result.of(() -> OBJECT_MAPPER.readValue(new FileInputStream(file), clazz));
    }

    public static <T> Result<T> deserialize(String body, Class<T> clazz) {
        return Result.of(() -> OBJECT_MAPPER.readValue(body, clazz));
    }

    public static <T> Result<String> serialize(T object) {
        return Result.of(() -> OBJECT_MAPPER.writeValueAsString(object));
    }

}
