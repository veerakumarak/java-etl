//package io.github.veerakumarak.etl.parquet;
//
//import org.apache.hadoop.conf.Configuration;
//
//import java.nio.file.Path;
//import java.util.function.Function;
//
//public class ParquetStreamProcessor {
//    public static <T, R> void process(Path src, Path dest, Class<T> srcClass, Class<R> resClass,
//                                      Function<T, R> mapper, Configuration conf) throws Exception {
//
//        // 1. Reader uses the schema existing in the file
//        try (var reader = AvroParquetReader.<GenericRecord>builder(src).withConf(conf).build()) {
//
//            // 2. We derive the output schema from the Result Class (R)
//            Schema outputSchema = ReflectData.get().getSchema(resClass);
//
//            try (var writer = AvroParquetWriter.<GenericRecord>builder(dest)
//                    .withSchema(outputSchema)
//                    .withConf(conf)
//                    .build()) {
//
//                GenericRecord inputAvro;
//                while ((inputAvro = reader.read()) != null) {
//                    // Convert: Avro -> Source POJO (T)
//                    T sourcePojo = MapperUtils.toPojo(inputAvro, srcClass);
//
//                    // Transform: T -> R
//                    R resultPojo = mapper.apply(sourcePojo);
//
//                    // Convert: Result POJO (R) -> Avro
//                    GenericRecord outputAvro = MapperUtils.toAvro(resultPojo, outputSchema);
//                    writer.write(outputAvro);
//                }
//            }
//        }
//    }
//}