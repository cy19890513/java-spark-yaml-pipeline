package com.github.cy19890513.etl.api;

import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * A stage that derives a new DataFrame from one or more upstream DataFrames.
 *
 * <p>The {@code inputs} map is keyed by upstream stage name, in the order the
 * pipeline definition declares them. Most transforms consume a single input.
 */
public interface TransformStage extends Stage {

    /**
     * Transforms upstream DataFrames into a new DataFrame.
     *
     * @param inputs  upstream DataFrames keyed by stage name, never null or empty
     * @param options stage options from the pipeline definition
     *                (e.g. {@code sql}, {@code condition})
     * @return the transformed DataFrame, never null
     */
    Dataset<Row> transform(Map<String, Dataset<Row>> inputs, Map<String, String> options);
}
