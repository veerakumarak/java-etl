package io.github.veerakumarak.etl.file;

import io.github.veerakumarak.fp.Result;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

public class S3FileLoader implements FileLoader {

    private final S3Client s3Client = S3Client.create();

    @Override
    public boolean canHandle(String path) {
        return path.startsWith("s3://");
    }

    @Override
    public Result<String> read(String path) {
        return Result.of(() -> {
            // Parse "s3://bucket-name/key"
            String uri = path.replace("s3://", "");
            String bucket = uri.substring(0, uri.indexOf("/"));
            String key = uri.substring(uri.indexOf("/") + 1);

            ResponseBytes<GetObjectResponse> objectBytes = s3Client.getObjectAsBytes(
                    builder -> builder.bucket(bucket).key(key)
            );
            return objectBytes.asUtf8String();
        });
    }
}