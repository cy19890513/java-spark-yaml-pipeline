package com.github.cy19890513.etl.connectors;

import com.github.cy19890513.etl.api.StageRegistry;

/**
 * Registers every built-in stage implementation with a {@link StageRegistry}.
 */
public final class BuiltinStages {

    private BuiltinStages() {
        // utility class
    }

    /**
     * Registers the built-in sources ({@code csv}, {@code parquet},
     * {@code jdbc}), the {@code sql} transform, and the built-in sinks
     * ({@code csv}, {@code parquet}, {@code jdbc}).
     *
     * @param registry the registry to populate
     */
    public static void registerAll(StageRegistry registry) {
        registry.registerSource("csv", CsvSource::new);
        registry.registerSource("parquet", ParquetSource::new);
        registry.registerSource("jdbc", JdbcSource::new);

        registry.registerTransform("sql", SqlTransform::new);

        registry.registerSink("csv", CsvSink::new);
        registry.registerSink("parquet", ParquetSink::new);
        registry.registerSink("jdbc", JdbcSink::new);
    }
}
