package io.github.veerakumarak.etl.compare;

import java.util.Objects;

public record ComparisonResult(
        boolean isMatch,
        long rowCountA,
        long rowCountB,
        long mismatchCount,
        String mismatchReason
) {
    public static ComparisonResult match(long count) {
        return new ComparisonResult(true, count, count, 0,null);
    }
    public static ComparisonResult mismatch(long countA, long countB, long mismatchCount, String reason) {
        return new ComparisonResult(false, countA, countB, mismatchCount, reason);
    }
    public ComparisonResult combine(ComparisonResult result) {
        if (this.isMatch && result.isMatch) {
            return match(this.rowCountA + result.rowCountA);
        } else {
            return mismatch(
                    this.rowCountA + result.rowCountA,
                    this.rowCountB+ result.rowCountB,
                    this.mismatchCount + result.mismatchCount,
                    Objects.nonNull(this.mismatchReason) ? this.mismatchReason : result.mismatchReason
            );
        }

    }
}