package com.github.cy19890513.etl.quality;

import com.github.cy19890513.etl.api.QualityCheck;
import com.github.cy19890513.etl.api.QualityResult;
import com.github.cy19890513.etl.api.Severity;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Fails when the row count falls outside the accepted range.
 *
 * <p>Optional options: {@code min} (default 0), {@code max}
 * (default unbounded).
 */
public class RowCountCheck implements QualityCheck {

    @Override
    public String name() {
        return "row_count";
    }

    @Override
    public String type() {
        return "row_count";
    }

    @Override
    public QualityResult check(Dataset<Row> input, Map<String, String> options) {
        long min = parseBound("row_count", options, "min", 0L);
        long max = parseBound("row_count", options, "max", Long.MAX_VALUE);
        if (min > max) {
            throw new IllegalArgumentException(
                    "The 'row_count' check requires min <= max, got min=" + min + ", max=" + max);
        }

        long count = input.count();
        if (count < min || count > max) {
            return QualityResult.fail("row_count",
                    "row count " + count + " outside [" + min + ", "
                            + (max == Long.MAX_VALUE ? "unbounded" : max) + "]",
                    Severity.FAIL);
        }
        return QualityResult.pass("row_count", "row count " + count + " within range");
    }

    private static long parseBound(String check, Map<String, String> options,
            String key, long defaultValue) {
        String value = options.get(key);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "The '" + check + "' check requires a numeric '" + key + "', got '" + value + "'");
        }
    }
}
