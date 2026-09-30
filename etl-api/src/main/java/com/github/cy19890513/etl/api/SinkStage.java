package com.github.cy19890513.etl.api;

import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * A stage that persists a DataFrame to an external system.
 *
 * <p>Implementations are looked up by type in the {@link StageRegistry},
 * e.g. {@code "parquet"}, {@code "csv"}, {@code "jdbc"}.
 */
public interface SinkStage extends Stage {

    /**
     * Writes the DataFrame to the external system.
     *
     * @param input   DataFrame to write, never null
     * @param options stage options from the pipeline definition
     *                (e.g. {@code path}, {@code mode}, {@code partitionBy})
     */
    void write(Dataset<Row> input, Map<String, String> options);
}
