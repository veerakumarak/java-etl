package io.github.veerakumarak.etl.parquet;

import junit.framework.TestCase;
import junit.framework.TestSuite;
import junit.framework.Test;
import org.apache.parquet.io.api.Binary;

import java.sql.Timestamp;

public class Int96TimestampUtilTest extends TestCase {

    public Int96TimestampUtilTest(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(Int96TimestampUtilTest.class);
    }

    public void testRoundTrip_normalTimestamp() {
        Timestamp original = Timestamp.valueOf("2026-10-06 14:30:45.123456789");
        Binary int96 = Int96TimestampUtil.toInt96(original);
        Timestamp result = Int96TimestampUtil.fromInt96(int96);

        assertEquals(original.getTime(), result.getTime());
        assertEquals(original.getNanos(), result.getNanos());
    }

    public void testRoundTrip_epochZero() {
        Timestamp original = new Timestamp(0L);
        Binary int96 = Int96TimestampUtil.toInt96(original);
        Timestamp result = Int96TimestampUtil.fromInt96(int96);

        assertEquals(0L, result.getTime());
        assertEquals(0, result.getNanos());
    }

    public void testRoundTrip_midnight() {
        Timestamp original = Timestamp.valueOf("2026-10-06 00:00:00.0");
        Binary int96 = Int96TimestampUtil.toInt96(original);
        Timestamp result = Int96TimestampUtil.fromInt96(int96);

        assertEquals(original.getTime(), result.getTime());
        assertEquals(0, result.getNanos());
    }

    public void testRoundTrip_endOfDay() {
        Timestamp original = Timestamp.valueOf("2026-10-06 23:59:59.999999999");
        Binary int96 = Int96TimestampUtil.toInt96(original);
        Timestamp result = Int96TimestampUtil.fromInt96(int96);

        assertEquals(original.getTime(), result.getTime());
        assertEquals(original.getNanos(), result.getNanos());
    }

    public void testRoundTrip_preEpoch() {
        Timestamp original = Timestamp.valueOf("1969-12-31 23:59:59.0");
        Binary int96 = Int96TimestampUtil.toInt96(original);
        Timestamp result = Int96TimestampUtil.fromInt96(int96);

        assertEquals(original.getTime(), result.getTime());
    }

    public void testRoundTrip_noNanos() {
        Timestamp original = Timestamp.valueOf("2026-10-06 10:00:00.0");
        Binary int96 = Int96TimestampUtil.toInt96(original);
        Timestamp result = Int96TimestampUtil.fromInt96(int96);

        assertEquals(original.getTime(), result.getTime());
        assertEquals(0, result.getNanos());
    }

    public void testRoundTrip_onlyMillis() {
        Timestamp original = Timestamp.valueOf("2026-10-06 14:30:45.123");
        Binary int96 = Int96TimestampUtil.toInt96(original);
        Timestamp result = Int96TimestampUtil.fromInt96(int96);

        assertEquals(original.getTime(), result.getTime());
        assertEquals(123000000, result.getNanos());
    }

    public void testRoundTrip_microsecondPrecision() {
        Timestamp original = Timestamp.valueOf("2026-10-06 14:30:45.123456");
        Binary int96 = Int96TimestampUtil.toInt96(original);
        Timestamp result = Int96TimestampUtil.fromInt96(int96);

        assertEquals(original.getTime(), result.getTime());
        assertEquals(123456000, result.getNanos());
    }

    public void testInt96_is12Bytes() {
        Timestamp ts = Timestamp.valueOf("2026-10-06 14:30:45.0");
        Binary int96 = Int96TimestampUtil.toInt96(ts);

        assertEquals(12, int96.length());
    }

    public void testJulianDay_isCorrect() {
        Timestamp ts = Timestamp.valueOf("2026-10-06 12:00:00.0");
        Binary int96 = Int96TimestampUtil.toInt96(ts);
        byte[] bytes = int96.getBytes();

        int julianDay = (bytes[8] & 0xFF)
                | ((bytes[9] & 0xFF) << 8)
                | ((bytes[10] & 0xFF) << 16)
                | ((bytes[11] & 0xFF) << 24);

        long daysSinceEpoch = ts.toLocalDateTime().toLocalDate().toEpochDay();
        int expectedJulian = (int) (daysSinceEpoch + 2440588L);
        assertEquals(expectedJulian, julianDay);
    }
}
