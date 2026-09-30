package com.github.cy19890513.etl.api;

import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * A data quality check evaluated against a DataFrame.
 *
 * <p>Checks never mutate data; they only report a {@link QualityResult}.
 * The pipeline runner decides what to do with a failing check based on its
 * {@link Severity}: {@code WARN} logs the problem and continues,
 * {@code FAIL} aborts the pipeline before anything is written.
 */
public interface QualityCheck extends Stage {

    /**
     * Evaluates the check against the given DataFrame.
     *
     * @param input   DataFrame to validate, never null
     * @param options check options from the pipeline definition
     *                (e.g. {@code columns}, {@code threshold})
     * @return the check result, never null
     */
    QualityResult check(Dataset<Row> input, Map<String, String> options);
}
