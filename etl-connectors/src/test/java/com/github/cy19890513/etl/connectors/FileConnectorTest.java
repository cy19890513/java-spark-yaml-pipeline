package com.github.cy19890513.etl.connectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * CSV and Parquet round-trip tests on a local Spark session.
 */
class FileConnectorTest {

    private static SparkSession spark;

    @TempDir
    static Path tempDir;

    @BeforeAll
    static void startSpark() {
        spark = SparkSession.builder()
                .appName("file-connector-test")
                .master("local[2]")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.shuffle.partitions", "2")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
    }

    @AfterAll
    static void stopSpark() {
        if (spark != null) {
            spark.stop();
        }
    }

    @Test
    void csvRoundTrip() throws Exception {
        Path input = tempDir.resolve("users.csv");
        Files.writeString(input, "id,name\n1,ann\n2,bob\n");
        Path output = tempDir.resolve("users-out");

        Dataset<Row> read = new CsvSource().read(
                spark, Map.of("path", input.toString(), "header", "true", "inferSchema", "true"));
        assertEquals(2, read.count());
        assertEquals("ann", read.filter("id = 1").select("name").first().getString(0));

        new CsvSink().write(read, Map.of(
                "path", output.toString(), "header", "true", "mode", "overwrite"));

        Dataset<Row> back = new CsvSource().read(
                spark, Map.of("path", output.toString(), "header", "true", "inferSchema", "true"));
        assertEquals(2, back.count());
    }

    @Test
    void parquetRoundTrip() {
        Path output = tempDir.resolve("events-parquet");

        Dataset<Row> original = spark.range(10).toDF("id");
        new ParquetSink().write(original,
                Map.of("path", output.toString(), "mode", "overwrite"));

        Dataset<Row> read = new ParquetSource().read(
                spark, Map.of("path", output.toString()));
        assertEquals(10, read.count());
    }

    @Test
    void parquetPartitionBy() {
        Path output = tempDir.resolve("partitioned-parquet");

        Dataset<Row> original = spark.range(4).toDF("id")
                .withColumn("grp", org.apache.spark.sql.functions.col("id").mod(2));
        new ParquetSink().write(original, Map.of(
                "path", output.toString(), "mode", "overwrite", "partitionBy", "[grp]"));

        long partitions;
        try (var stream = Files.list(output)) {
            partitions = stream.filter(p -> p.getFileName().toString().startsWith("grp=")).count();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        assertEquals(2, partitions);
    }

    @Test
    void sqlTransformFilters() {
        Dataset<Row> input = spark.range(5).toDF("id");

        Dataset<Row> result = new SqlTransform().transform(
                Map.of("raw", input),
                Map.of("sql", "SELECT id FROM __input__ WHERE id > 2"));

        assertEquals(2, result.count());
    }

    @Test
    void sqlTransformWithTwoInputs() {
        Dataset<Row> left = spark.range(3).toDF("id");
        Dataset<Row> right = spark.range(3).toDF("id");

        Dataset<Row> result = new SqlTransform().transform(
                Map.of("left", left, "right", right),
                Map.of("sql", "SELECT l.id AS a, r.id AS b FROM left l JOIN right r ON l.id = r.id"));

        assertEquals(3, result.count());
    }
}
