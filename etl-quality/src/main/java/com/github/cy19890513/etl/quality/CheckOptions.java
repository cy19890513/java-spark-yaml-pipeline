package com.github.cy19890513.etl.quality;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Shared helpers for the built-in quality checks.
 */
final class CheckOptions {

    private CheckOptions() {
        // utility class
    }

    /**
     * Returns the required {@code columns} option as a list.
     * Accepts {@code [a, b]}, {@code a,b}, or a single {@code a}.
     *
     * @param check   check type for the error message, e.g. {@code "not_null"}
     * @param options check options
     * @return the column names, never empty
     * @throws IllegalArgumentException when the option is missing or blank
     */
    static List<String> requireColumns(String check, Map<String, String> options) {
        String value = options.get("columns");
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(
                    "The '" + check + "' check requires option 'columns'");
        }
        String trimmed = value.trim();
        if (trimmed.startsWith("[") && trimmed.endsWith("]")) {
            trimmed = trimmed.substring(1, trimmed.length() - 1);
        }
        List<String> columns = new ArrayList<>();
        for (String part : trimmed.split(",")) {
            String column = part.trim();
            if (!column.isEmpty()) {
                columns.add(column);
            }
        }
        if (columns.isEmpty()) {
            throw new IllegalArgumentException(
                    "The '" + check + "' check requires at least one column");
        }
        return columns;
    }
}
