package com.github.cy19890513.etl.quality;

import com.github.cy19890513.etl.api.QualityCheck;
import com.github.cy19890513.etl.api.QualityResult;
import com.github.cy19890513.etl.api.Severity;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Fails when the given column combination contains duplicate values.
 *
 * <p>Required options: {@code columns} (e.g. {@code [id]} or
 * {@code [order_id, line_no]}). Reports how many duplicate groups exist.
 */
public class UniqueCheck implements QualityCheck {

    @Override
    public String name() {
        return "unique";
    }

    @Override
    public String type() {
        return "unique";
    }

    @Override
    public QualityResult check(Dataset<Row> input, Map<String, String> options) {
        List<String> columns = CheckOptions.requireColumns("unique", options);

        long duplicateGroups = input
                .groupBy(columns.get(0), columns.subList(1, columns.size()).toArray(new String[0]))
                .count()
                .filter("count > 1")
                .count();

        if (duplicateGroups == 0) {
            return QualityResult.pass("unique", "0 duplicates in " + columns);
        }
        return QualityResult.fail("unique",
                duplicateGroups + " duplicate group(s) in " + columns, Severity.FAIL);
    }
}
