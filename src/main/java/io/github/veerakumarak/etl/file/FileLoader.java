package io.github.veerakumarak.etl.file;

import io.github.veerakumarak.fp.Result;

public interface FileLoader {
    boolean canHandle(String path);
    Result<String> read(String path);
}