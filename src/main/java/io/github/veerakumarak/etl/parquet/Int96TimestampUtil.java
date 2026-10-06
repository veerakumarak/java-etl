package io.github.veerakumarak.etl.parquet;

import org.apache.parquet.io.api.Binary;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

public class Int96TimestampUtil {

    private static final long JULIAN_EPOCH_OFFSET_DAYS = 2440588L;

    public static Binary toInt96(Timestamp ts) {
        long millisSinceEpoch = ts.getTime();
        int nanos = ts.getNanos();

        long daysSinceEpoch = Math.floorDiv(millisSinceEpoch, TimeUnit.DAYS.toMillis(1));
        int julianDay = (int) (daysSinceEpoch + JULIAN_EPOCH_OFFSET_DAYS);

        long millisOfDay = millisSinceEpoch - daysSinceEpoch * TimeUnit.DAYS.toMillis(1);
        long nanosOfDay = TimeUnit.MILLISECONDS.toNanos(millisOfDay)
                + (nanos % 1_000_000);

        ByteBuffer buf = ByteBuffer.allocate(12).order(ByteOrder.LITTLE_ENDIAN);
        buf.putLong(nanosOfDay);
        buf.putInt(julianDay);
        buf.flip();

        return Binary.fromConstantByteArray(buf.array());
    }

    public static Timestamp fromInt96(Binary int96) {
        ByteBuffer buf = int96.toByteBuffer().order(ByteOrder.LITTLE_ENDIAN);
        long nanosOfDay = buf.getLong();
        int julianDay = buf.getInt();

        long daysSinceEpoch = julianDay - JULIAN_EPOCH_OFFSET_DAYS;
        long millisSinceEpoch = daysSinceEpoch * TimeUnit.DAYS.toMillis(1)
                + nanosOfDay / 1_000_000;
        int remainingNanos = (int) (nanosOfDay % 1_000_000_000);

        Timestamp ts = new Timestamp(millisSinceEpoch);
        ts.setNanos(remainingNanos);
        return ts;
    }
}
