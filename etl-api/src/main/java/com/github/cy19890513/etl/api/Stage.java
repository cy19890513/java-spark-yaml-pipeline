package com.github.cy19890513.etl.api;

/**
 * Base contract for every executable unit of a pipeline.
 *
 * <p>A stage has a logical {@code name} (unique within a pipeline, used to
 * wire stages together) and a {@code type} (used to look the implementation
 * up in the {@link StageRegistry}).
 */
public interface Stage {

    /**
     * Returns the logical name of this stage, unique within its pipeline.
     *
     * @return stage name, never null
     */
    String name();

    /**
     * Returns the registered type of this stage, e.g. {@code "csv"},
     * {@code "sql"}, {@code "not_null"}.
     *
     * @return stage type, never null
     */
    String type();
}
