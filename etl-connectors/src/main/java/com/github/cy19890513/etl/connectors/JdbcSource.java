package com.github.cy19890513.etl.connectors;

import com.github.cy19890513.etl.api.SourceStage;
import java.util.Map;
import java.util.Set;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Reads a table through JDBC. Every option besides {@code url} and
 * {@code dbtable} is passed to the Spark JDBC reader as-is (e.g.
 * {@code user}, {@code password}, {@code driver}, {@code fetchsize}).
 *
 * <p>Required options: {@code url}, {@code dbtable}.
 */
public class JdbcSource implements SourceStage {

    private static final Set<String> CONSUMED = Set.of("url", "dbtable");

    @Override
    public String name() {
        return "jdbc";
    }

    @Override
    public String type() {
        return "jdbc";
    }

    @Override
    public Dataset<Row> read(SparkSession spark, Map<String, String> options) {
        String url = ConnectorOptions.require("jdbc source", options, "url");
        String dbtable = ConnectorOptions.require("jdbc source", options, "dbtable");
        var reader = spark.read().format("jdbc")
                .option("url", url)
                .option("dbtable", dbtable);
        for (Map.Entry<String, String> entry : options.entrySet()) {
            if (!CONSUMED.contains(entry.getKey())) {
                reader = reader.option(entry.getKey(), entry.getValue());
            }
        }
        return reader.load();
    }
}
