package com.github.cy19890513.etl.connectors;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Shared helpers for the built-in connector implementations.
 */
final class ConnectorOptions {

    private ConnectorOptions() {
        // utility class
    }

    /**
     * Returns a required option or throws.
     *
     * @param stage   stage type for the error message, e.g. {@code "csv source"}
     * @param options stage options
     * @param key     required key
     * @return the option value, never blank
     * @throws IllegalArgumentException when the option is missing or blank
     */
    static String require(String stage, Map<String, String> options, String key) {
        String value = options.get(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(
                    "The '" + stage + "' stage requires option '" + key + "'");
        }
        return value;
    }

    /**
     * Parses a list-valued option such as {@code partitionBy}.
     * Accepts {@code [a, b]}, {@code a,b}, or a single {@code a}.
     *
     * @param value raw option value
     * @return the parsed items, never empty
     */
    static List<String> parseList(String value) {
        String trimmed = value.trim();
        if (trimmed.startsWith("[") && trimmed.endsWith("]")) {
            trimmed = trimmed.substring(1, trimmed.length() - 1);
        }
        List<String> items = new ArrayList<>();
        for (String part : trimmed.split(",")) {
            String item = part.trim();
            if (!item.isEmpty()) {
                items.add(item);
            }
        }
        return items;
    }
}
