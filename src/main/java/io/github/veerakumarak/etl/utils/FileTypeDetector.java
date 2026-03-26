package io.github.veerakumarak.etl.utils;

import io.github.veerakumarak.fp.Result;
import io.github.veerakumarak.fp.failures.IllegalArgument;

public class FileTypeDetector {

    private static String getExtension(String path) {
        if (path == null || !path.contains(".")) {
            return "";
        }
        return path.substring(path.lastIndexOf(".") + 1).toLowerCase();
    }

    public static Result<FileType> detectType(String path) {
        return Result.of(() -> {
            String ext = getExtension(path);

            return switch (ext) {
                case "parquet" -> FileType.PARQUET;
                case "csv"     -> FileType.CSV;
                case "json"    -> FileType.JSON;
                case "avro"    -> FileType.AVRO;
                case "txt"     -> FileType.TEXT;
                default        -> throw new IllegalArgument("Unsupported file type: " + ext);
            };
        });
    }
}
