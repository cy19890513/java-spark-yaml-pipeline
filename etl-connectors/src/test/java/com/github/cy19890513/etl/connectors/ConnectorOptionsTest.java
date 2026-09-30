package com.github.cy19890513.etl.connectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ConnectorOptions} (no Spark needed).
 */
class ConnectorOptionsTest {

    @Test
    void requireReturnsValue() {
        assertEquals("x", ConnectorOptions.require("csv source", Map.of("path", "x"), "path"));
    }

    @Test
    void requireThrowsOnMissing() {
        assertThrows(IllegalArgumentException.class,
                () -> ConnectorOptions.require("csv source", Map.of(), "path"));
    }

    @Test
    void requireThrowsOnBlank() {
        assertThrows(IllegalArgumentException.class,
                () -> ConnectorOptions.require("csv source", Map.of("path", "  "), "path"));
    }

    @Test
    void parseListHandlesBrackets() {
        assertEquals(List.of("dt", "hr"), ConnectorOptions.parseList("[dt, hr]"));
    }

    @Test
    void parseListHandlesPlainCsv() {
        assertEquals(List.of("dt", "hr"), ConnectorOptions.parseList("dt,hr"));
    }

    @Test
    void parseListHandlesSingle() {
        assertEquals(List.of("dt"), ConnectorOptions.parseList("dt"));
    }
}
