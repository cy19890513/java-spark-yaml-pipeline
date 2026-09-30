package com.github.cy19890513.etl.api;

import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * A stage that produces a DataFrame from an external system.
 *
 * <p>Implementations are looked up by type in the {@link StageRegistry},
 * e.g. {@code "csv"}, {@code "parquet"}, {@code "jdbc"}.
 */
public interface SourceStage extends Stage {

    /**
     * Reads data from the external system.
     *
     * @param spark   active Spark session
     * @param options stage options from the pipeline definition
     *                (e.g. {@code path}, {@code url}, {@code dbtable})
     * @return the loaded DataFrame, never null
     */
    Dataset<Row> read(SparkSession spark, Map<String, String> options);
}
