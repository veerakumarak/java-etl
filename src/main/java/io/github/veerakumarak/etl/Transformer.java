//package io.github.veerakumarak.etl;
//
//import java.util.function.Function;
//
//import io.github.veerakumarak.etl.parquet.ParquetAwsManager;
//import io.github.veerakumarak.etl.parquet.ParquetStreamProcessor;
//import org.apache.hadoop.fs.Path;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//public class Transformer<T> {
//
//    private static final Logger log = LoggerFactory.getLogger(Transformer.class);
//
//    private final String sourcePath;
//    private final Class<T> sourceClass;
//
//    private Transformer(String sourcePath, Class<T> sourceClass) {
//        this.sourcePath = sourcePath;
//        this.sourceClass = sourceClass;
//    }
//
//    public static <T> Transformer<T> fromParquet(String path, Class<T> clazz) {
//        return new Transformer<>(path, clazz);
//    }
//
//    // This method shifts the Generic context from T to R
//    public <R> MapPhase<T, R> transform(Function<T, R> logic, Class<R> resultClass) {
//        return new MapPhase<>(sourcePath, sourceClass, logic, resultClass);
//    }
//
//    public static class MapPhase<T, R> {
//        private final String sourcePath;
//        private final Class<T> sourceClass;
//        private final Function<T, R> logic;
//        private final Class<R> resultClass;
//
//        public MapPhase(String source, Class<T> srcClass, Function<T, R> logic, Class<R> resClass) {
//            this.sourcePath = source;
//            this.sourceClass = srcClass;
//            this.logic = logic;
//            this.resultClass = resClass;
//        }
//
//        public void toParquet(String destinationPath) throws Exception {
//            var conf = ParquetAwsManager.getConfiguration();
//            // Path automatically handles s3a:// or file:///
//            ParquetStreamProcessor.process(
//                    new Path(sourcePath),
//                    new Path(destinationPath),
//                    sourceClass,
//                    resultClass,
//                    logic,
//                    conf
//            );
//        }
//    }
//}