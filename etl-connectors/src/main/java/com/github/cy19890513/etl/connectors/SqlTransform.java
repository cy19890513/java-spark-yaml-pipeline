package com.github.cy19890513.etl.connectors;

import com.github.cy19890513.etl.api.TransformStage;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Applies a Spark SQL query to the inputs.
 *
 * <p>Each input DataFrame is registered as a temporary view named after its
 * stage (sanitized to {@code [A-Za-z0-9_]}). With a single input, the
 * {@code __input__} placeholder in the SQL is replaced by that view name,
 * so {@code SELECT * FROM __input__ WHERE age &gt;= 18} just works.
 * With several inputs, reference the stage-named views directly.
 *
 * <p>Required options: {@code sql}.
 */
public class SqlTransform implements TransformStage {

    @Override
    public String name() {
        return "sql";
    }

    @Override
    public String type() {
        return "sql";
    }

    @Override
    public Dataset<Row> transform(Map<String, Dataset<Row>> inputs, Map<String, String> options) {
        String sql = ConnectorOptions.require("sql transform", options, "sql");
        if (inputs.isEmpty()) {
            throw new IllegalArgumentException("The 'sql' transform requires at least one input");
        }
        SparkSession spark = inputs.values().iterator().next().sparkSession();
        Map<String, String> views = new LinkedHashMap<>();
        for (Map.Entry<String, Dataset<Row>> entry : inputs.entrySet()) {
            String view = sanitize(entry.getKey());
            entry.getValue().createOrReplaceTempView(view);
            views.put(entry.getKey(), view);
        }
        if (views.size() == 1) {
            sql = sql.replace("__input__", views.values().iterator().next());
        }
        return spark.sql(sql);
    }

    private static String sanitize(String name) {
        return name.replaceAll("[^A-Za-z0-9_]", "_");
    }
}
