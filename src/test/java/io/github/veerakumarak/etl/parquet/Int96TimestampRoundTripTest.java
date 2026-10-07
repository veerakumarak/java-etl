package io.github.veerakumarak.etl.parquet;

import io.github.veerakumarak.etl.entities.FileMetaData;
import io.github.veerakumarak.etl.sink.ParquetWriterHelper;
import io.github.veerakumarak.etl.source.ParquetReaderHelper;
import io.github.veerakumarak.etl.utils.DateUtil;
import io.github.veerakumarak.fp.Result;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Method;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies that timestamps written in the legacy INT96 format (for older Spark readers) are physically
 * stored as the INT96 primitive and survive a write -> read round-trip through the POJO path.
 */
class Int96TimestampRoundTripTest {

    /** Simple POJO with an all-args constructor, as required by the reader. */
    public static class Event {
        private final String name;
        private final LocalDateTime occurredAt;

        public Event(String name, LocalDateTime occurredAt) {
            this.name = name;
            this.occurredAt = occurredAt;
        }

        public String getName() {
            return name;
        }

        public LocalDateTime getOccurredAt() {
            return occurredAt;
        }
    }

    private static final ZoneId ZONE = ZoneId.of("America/New_York");
    private static final String TABLE = "Event";

    @Test
    void writesInt96AndReadsBackSameTimestamp(@TempDir Path tempDir) throws Exception {
        LocalDateTime ts = LocalDateTime.of(2019, 3, 15, 13, 45, 30, 123_000_000);
        Event input = new Event("launch", ts);

        String writePath = tempDir.toString();

        // Write with the opt-in INT96 timestamp format.
        FileMetaData meta = invokeWriteBatched(writePath, List.of(input), ZONE);
        assertEquals(1, meta.generatedFiles().size(), "expected exactly one output file");

        String outputFile = meta.generatedFiles().iterator().next();

        // 1. Assert the physical schema stored the timestamp as INT96 (the core of the fix).
        MessageType schema = ParquetReaderHelper.readSchema(outputFile, new Configuration()).orElseThrow();
        PrimitiveType tsField = schema.getType("occurredAt").asPrimitiveType();
        assertEquals(PrimitiveType.PrimitiveTypeName.INT96, tsField.getPrimitiveTypeName(),
                "timestamp column should be written as INT96");

        // 2. Assert the value round-trips back to the original wall-clock LocalDateTime.
        List<Event> readBack = ParquetReaderHelper.readList(outputFile, Event.class, ZONE).orElseThrow();
        assertEquals(1, readBack.size());
        assertEquals("launch", readBack.get(0).getName());
        assertEquals(ts, readBack.get(0).getOccurredAt(),
                "INT96 timestamp should survive the write/read round-trip unchanged");
    }

    @Test
    void int96EncodingDecodingIsInverse() {
        LocalDateTime original = LocalDateTime.of(2007, 12, 31, 23, 59, 59, 999_999_000);
        long[] int96 = DateUtil.localDateTimeToInt96(original);
        LocalDateTime decoded = DateUtil.int96ToLocalDateTime((int) int96[0], int96[1]);
        assertEquals(original, decoded, "localDateTimeToInt96 and int96ToLocalDateTime must be inverses");
    }

    @Test
    void int96DecodesPreEpochDates() {
        // A date before 1970 exercises the floorDiv/floorMod paths.
        LocalDateTime original = LocalDateTime.of(1960, 6, 1, 8, 30, 0, 0);
        long[] int96 = DateUtil.localDateTimeToInt96(original);
        assertTrue(int96[0] > 0, "Julian day should be positive for 1960");
        assertEquals(original, DateUtil.int96ToLocalDateTime((int) int96[0], int96[1]));
    }

    /**
     * {@code ParquetWriterHelper.writeBatched(...)} is package-protected, so reflection is used to call the
     * INT96-aware overload from this test's package without widening the production API's visibility.
     */
    @SuppressWarnings("unchecked")
    private static FileMetaData invokeWriteBatched(String writePath, List<Event> data, ZoneId zone) throws Exception {
        Method m = ParquetWriterHelper.class.getDeclaredMethod(
                "writeBatched", String.class, String.class, Integer.class, Stream.class, Class.class,
                List.class, boolean.class, ZoneId.class);
        m.setAccessible(true);
        Result<FileMetaData> result = (Result<FileMetaData>) m.invoke(null, writePath, TABLE, 100,
                data.stream(), Event.class, List.of(), true, zone);
        return result.orElseThrow();
    }
}
