package com.github.cy19890513.etl.quality;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.cy19890513.etl.api.QualityResult;
import com.github.cy19890513.etl.api.Severity;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Tests for the built-in quality checks on a local Spark session.
 */
class QualityCheckTest {

    private static SparkSession spark;

    @BeforeAll
    static void startSpark() {
        spark = SparkSession.builder()
                .appName("quality-check-test")
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

    private static Dataset<Row> users() {
        StructType schema = new StructType()
                .add("id", DataTypes.IntegerType, false)
                .add("name", DataTypes.StringType, true);
        List<Row> rows = Arrays.asList(
                RowFactory.create(1, "ann"),
                RowFactory.create(2, null),
                RowFactory.create(2, "bob"));
        return spark.createDataFrame(rows, schema);
    }

    @Test
    void notNullPassesWithoutNulls() {
        Dataset<Row> input = users().filter("name IS NOT NULL");

        QualityResult result = new NotNullCheck().check(input, Map.of("columns", "[id, name]"));

        assertTrue(result.passed());
    }

    @Test
    void notNullFailsWithNulls() {
        QualityResult result = new NotNullCheck().check(users(), Map.of("columns", "name"));

        assertTrue(!result.passed());
        assertEquals(Severity.FAIL, result.severity());
        assertTrue(result.message().contains("1 null(s) in [name]"));
    }

    @Test
    void notNullRequiresColumns() {
        assertThrows(IllegalArgumentException.class,
                () -> new NotNullCheck().check(users(), Map.of()));
    }

    @Test
    void uniquePassesWithoutDuplicates() {
        Dataset<Row> input = users().filter("id = 1");

        QualityResult result = new UniqueCheck().check(input, Map.of("columns", "[id]"));

        assertTrue(result.passed());
    }

    @Test
    void uniqueFailsWithDuplicates() {
        QualityResult result = new UniqueCheck().check(users(), Map.of("columns", "id"));

        assertTrue(!result.passed());
        assertTrue(result.message().contains("1 duplicate group(s)"));
    }

    @Test
    void rowCountPassesWithinRange() {
        QualityResult result = new RowCountCheck().check(
                users(), Map.of("min", "2", "max", "5"));

        assertTrue(result.passed());
    }

    @Test
    void rowCountFailsBelowMin() {
        QualityResult result = new RowCountCheck().check(users(), Map.of("min", "10"));

        assertTrue(!result.passed());
        assertTrue(result.message().contains("outside"));
    }

    @Test
    void rowCountFailsAboveMax() {
        QualityResult result = new RowCountCheck().check(users(), Map.of("max", "1"));

        assertTrue(!result.passed());
    }

    @Test
    void rowCountDefaultsAreSane() {
        QualityResult result = new RowCountCheck().check(users(), Map.of());

        assertTrue(result.passed());
    }

    @Test
    void rowCountRejectsNonNumericBound() {
        assertThrows(IllegalArgumentException.class,
                () -> new RowCountCheck().check(users(), Map.of("min", "many")));
    }
}
