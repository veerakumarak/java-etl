package io.github.veerakumarak.etl.source;

import io.github.veerakumarak.fp.Result;

import java.util.List;
import java.util.stream.Stream;

public class FileSource {
    public static <T> Result<Stream<T>> readStream(String filePath, Class<T> tClass, boolean relaxedValidation) {
        return ParquetReaderHelper.readStream(filePath, tClass, relaxedValidation);
    }
    public static <T> Result<List<T>> readList(String filePath, Class<T> tClass, boolean relaxedValidation) {
        return ParquetReaderHelper.readList(filePath, tClass, relaxedValidation);
    }
}
