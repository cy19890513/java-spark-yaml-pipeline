package com.github.cy19890513.etl.connectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Map;
import java.util.UUID;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * JDBC round-trip tests against an in-memory H2 database.
 */
class JdbcConnectorTest {

    private static SparkSession spark;

    @BeforeAll
    static void startSpark() {
        spark = SparkSession.builder()
                .appName("jdbc-connector-test")
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

    private static String freshUrl() {
        return "jdbc:h2:mem:db" + UUID.randomUUID().toString().replace("-", "") + ";DB_CLOSE_DELAY=-1";
    }

    private static void seed(String url) throws Exception {
        try (Connection conn = DriverManager.getConnection(url);
                Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE users(id INT PRIMARY KEY, name VARCHAR(50))");
            stmt.execute("INSERT INTO users VALUES (1, 'ann'), (2, 'bob')");
        }
    }

    @Test
    void jdbcRoundTrip() throws Exception {
        String url = freshUrl();
        seed(url);

        Map<String, String> sourceOptions = Map.of(
                "url", url,
                "dbtable", "users",
                "driver", "org.h2.Driver");
        Dataset<Row> read = new JdbcSource().read(spark, sourceOptions);
        assertEquals(2, read.count());

        new JdbcSink().write(read, Map.of(
                "url", url,
                "dbtable", "users_copy",
                "driver", "org.h2.Driver",
                "mode", "overwrite"));

        Dataset<Row> back = new JdbcSource().read(spark, Map.of(
                "url", url,
                "dbtable", "users_copy",
                "driver", "org.h2.Driver"));
        assertEquals(2, back.count());
        assertEquals("ann", back.filter("id = 1").select("name").first().getString(0));
    }

    @Test
    void jdbcSourceRequiresUrlAndTable() {
        assertThrows(IllegalArgumentException.class,
                () -> new JdbcSource().read(spark, Map.of("dbtable", "users")));
        assertThrows(IllegalArgumentException.class,
                () -> new JdbcSource().read(spark, Map.of("url", "jdbc:h2:mem:x")));
    }
}
