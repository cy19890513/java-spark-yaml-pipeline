package com.github.cy19890513.etl.connectors;

import com.github.cy19890513.etl.api.SinkStage;
import java.util.Map;
import java.util.Set;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Writes a DataFrame through JDBC. Every option besides {@code url},
 * {@code dbtable}, and {@code mode} is passed to the Spark JDBC writer
 * as-is (e.g. {@code user}, {@code password}, {@code driver},
 * {@code batchsize}).
 *
 * <p>Required options: {@code url}, {@code dbtable}. Optional:
 * {@code mode} (default {@code errorIfExists}; {@code overwrite},
 * {@code append}, and {@code ignore} are supported).
 */
public class JdbcSink implements SinkStage {

    private static final Set<String> CONSUMED = Set.of("url", "dbtable", "mode");

    @Override
    public String name() {
        return "jdbc";
    }

    @Override
    public String type() {
        return "jdbc";
    }

    @Override
    public void write(Dataset<Row> input, Map<String, String> options) {
        String url = ConnectorOptions.require("jdbc sink", options, "url");
        String dbtable = ConnectorOptions.require("jdbc sink", options, "dbtable");
        String mode = options.getOrDefault("mode", "errorIfExists");
        var writer = input.write().format("jdbc").mode(mode)
                .option("url", url)
                .option("dbtable", dbtable);
        for (Map.Entry<String, String> entry : options.entrySet()) {
            if (!CONSUMED.contains(entry.getKey())) {
                writer = writer.option(entry.getKey(), entry.getValue());
            }
        }
        writer.save();
    }
}
