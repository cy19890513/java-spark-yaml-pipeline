package com.github.cy19890513.etl.quality;

import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.when;

import com.github.cy19890513.etl.api.QualityCheck;
import com.github.cy19890513.etl.api.QualityResult;
import com.github.cy19890513.etl.api.Severity;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

/**
 * Fails when any of the given columns contains null values.
 *
 * <p>Required options: {@code columns} (e.g. {@code [id, email]}).
 * Reports per-column null counts in a single aggregation pass.
 */
public class NotNullCheck implements QualityCheck {

    @Override
    public String name() {
        return "not_null";
    }

    @Override
    public String type() {
        return "not_null";
    }

    @Override
    public QualityResult check(Dataset<Row> input, Map<String, String> options) {
        List<String> columns = CheckOptions.requireColumns("not_null", options);

        List<Column> aggregations = new ArrayList<>();
        for (String column : columns) {
            aggregations.add(sum(when(functions.col(column).isNull(), 1).otherwise(0)).as(column));
        }
        Row counts = input
                .agg(aggregations.get(0),
                        aggregations.subList(1, aggregations.size()).toArray(new Column[0]))
                .first();

        List<String> violations = new ArrayList<>();
        for (String column : columns) {
            long nulls = counts.getLong(counts.fieldIndex(column));
            if (nulls > 0) {
                violations.add(nulls + " null(s) in [" + column + "]");
            }
        }
        if (violations.isEmpty()) {
            return QualityResult.pass("not_null", "0 nulls in " + columns);
        }
        return QualityResult.fail("not_null", String.join(", ", violations), Severity.FAIL);
    }
}
