package io.github.veerakumarak.etl.parquet;

import org.apache.hadoop.conf.Configuration;

public class ParquetAwsManager {

    public static Configuration getConfiguration() {
        Configuration conf = new Configuration();
        conf.set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain");
        conf.set("fs.s3a.endpoint", "s3.amazonaws.com");

        // Use the V2 Tabular provider for Java 25 compatibility
//        conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.auth.AWSCredentialsProviderV2Tabular");
//        conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
//        conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");

        // Recommended for high-performance Mac/Linux local dev
//        conf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");

        return conf;
    }

    public static boolean isAwsPath(String input) {
        return input.startsWith("s3a://") || input.startsWith("s3://");
    }

    public static boolean isLocalPath(String input) {
        return input.startsWith("file://") || input.startsWith("/") || input.contains(":\\");
    }
}
