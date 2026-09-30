package com.github.cy19890513.etl.quality;

import com.github.cy19890513.etl.api.StageRegistry;

/**
 * Registers every built-in quality check with a {@link StageRegistry}.
 */
public final class QualityChecks {

    private QualityChecks() {
        // utility class
    }

    /**
     * Registers the built-in checks ({@code not_null}, {@code unique},
     * {@code row_count}).
     *
     * @param registry the registry to populate
     */
    public static void registerAll(StageRegistry registry) {
        registry.registerQualityCheck("not_null", NotNullCheck::new);
        registry.registerQualityCheck("unique", UniqueCheck::new);
        registry.registerQualityCheck("row_count", RowCountCheck::new);
    }
}
