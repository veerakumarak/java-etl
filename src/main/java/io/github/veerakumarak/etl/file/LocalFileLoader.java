package io.github.veerakumarak.etl.file;

import io.github.veerakumarak.fp.Result;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Enumeration;
import java.util.stream.Collectors;

public class LocalFileLoader implements FileLoader {
    @Override
    public boolean canHandle(String path) {
        return path.startsWith("file://");
    }

    @Override
    public Result<String> read(String path){

        InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(path);
        if (is == null) throw new IllegalArgumentException("SQL file not found: " + path);

        return Result.of(()-> new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))
                .lines().collect(Collectors.joining("\n")));

//        return Result.of(()-> Files.readString(Path.of(path)));
    }

    public static Result<String> read2(String path) throws IOException {
        Enumeration<URL> e = Thread.currentThread().getContextClassLoader().getResources("/");
        while (e.hasMoreElements())
        {
            System.out.println("ClassLoader Resource: " + e.nextElement());
        }
//        System.out.println("Class Resource: " + Thread.currentThread().getContextClassLoader().getResources("/"));


        System.out.println(Thread.currentThread().getContextClassLoader());
        URL resource = Thread.currentThread().getContextClassLoader().getResource("/");
        System.out.println(resource);
        InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(path);
        if (is == null) throw new IllegalArgumentException("SQL file not found: " + path);

        return Result.of(()-> new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))
                .lines().collect(Collectors.joining("\n")));
//        Path p = Path.of(path);
//        System.out.println(p);
//        return Result.of(()-> Files.readString(p));
    }

    static void main() throws IOException {
        System.out.println(read2("/Users/veerakumar/gitlab/distdir/dd-etl/hello.txt"));
    }

}