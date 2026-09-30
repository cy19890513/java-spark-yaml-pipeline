package com.github.cy19890513.etl.connectors;

import com.github.cy19890513.etl.api.SourceStage;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Reads a CSV file. Every option besides {@code path} is passed to the
 * Spark CSV reader as-is (e.g. {@code header}, {@code delimiter},
 * {@code inferSchema}).
 *
 * <p>Required options: {@code path}.
 */
public class CsvSource implements SourceStage {

    @Override
    public String name() {
        return "csv";
    }

    @Override
    public String type() {
        return "csv";
    }

    @Override
    public Dataset<Row> read(SparkSession spark, Map<String, String> options) {
        String path = ConnectorOptions.require("csv source", options, "path");
        var reader = spark.read().format("csv");
        for (Map.Entry<String, String> entry : options.entrySet()) {
            if (!entry.getKey().equals("path")) {
                reader = reader.option(entry.getKey(), entry.getValue());
            }
        }
        return reader.load(path);
    }
}
