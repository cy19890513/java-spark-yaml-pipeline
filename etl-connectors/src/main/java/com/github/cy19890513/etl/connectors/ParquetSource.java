package com.github.cy19890513.etl.connectors;

import com.github.cy19890513.etl.api.SourceStage;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Reads Parquet files. Every option besides {@code path} is passed to the
 * Spark reader as-is (e.g. {@code mergeSchema}).
 *
 * <p>Required options: {@code path}.
 */
public class ParquetSource implements SourceStage {

    @Override
    public String name() {
        return "parquet";
    }

    @Override
    public String type() {
        return "parquet";
    }

    @Override
    public Dataset<Row> read(SparkSession spark, Map<String, String> options) {
        String path = ConnectorOptions.require("parquet source", options, "path");
        var reader = spark.read().format("parquet");
        for (Map.Entry<String, String> entry : options.entrySet()) {
            if (!entry.getKey().equals("path")) {
                reader = reader.option(entry.getKey(), entry.getValue());
            }
        }
        return reader.load(path);
    }
}
