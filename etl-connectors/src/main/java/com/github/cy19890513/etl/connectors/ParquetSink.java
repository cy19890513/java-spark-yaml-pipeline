package com.github.cy19890513.etl.connectors;

import com.github.cy19890513.etl.api.SinkStage;
import java.util.Map;
import java.util.Set;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Writes a DataFrame as Parquet. Every option besides {@code path},
 * {@code mode}, and {@code partitionBy} is passed to the Spark writer
 * as-is (e.g. {@code compression}).
 *
 * <p>Required options: {@code path}. Optional: {@code mode} (default
 * {@code errorIfExists}), {@code partitionBy} (e.g. {@code [dt]}).
 */
public class ParquetSink implements SinkStage {

    private static final Set<String> CONSUMED = Set.of("path", "mode", "partitionBy");

    @Override
    public String name() {
        return "parquet";
    }

    @Override
    public String type() {
        return "parquet";
    }

    @Override
    public void write(Dataset<Row> input, Map<String, String> options) {
        String path = ConnectorOptions.require("parquet sink", options, "path");
        String mode = options.getOrDefault("mode", "errorIfExists");
        var writer = input.write().format("parquet").mode(mode);
        for (Map.Entry<String, String> entry : options.entrySet()) {
            if (!CONSUMED.contains(entry.getKey())) {
                writer = writer.option(entry.getKey(), entry.getValue());
            }
        }
        String partitionBy = options.get("partitionBy");
        if (partitionBy != null && !partitionBy.isBlank()) {
            writer = writer.partitionBy(ConnectorOptions.parseList(partitionBy).toArray(String[]::new));
        }
        writer.save(path);
    }
}
