package com.github.cy19890513.etl.core;

import java.util.Map;
import org.apache.spark.sql.SparkSession;

/**
 * Creates configured {@link SparkSession} instances for pipeline runs.
 */
public final class SparkSessionFactory {

    private SparkSessionFactory() {
        // utility class
    }

    /**
     * Creates a session with the given application name and no extra config.
     *
     * @param appName Spark application name, typically derived from the pipeline name
     * @return the new session
     */
    public static SparkSession create(String appName) {
        return create(appName, Map.of());
    }

    /**
     * Creates a session with the given application name and Spark config.
     *
     * @param appName   Spark application name
     * @param sparkConf extra {@code spark.*} settings, e.g. {@code spark.master}
     * @return the new session
     */
    public static SparkSession create(String appName, Map<String, String> sparkConf) {
        SparkSession.Builder builder = SparkSession.builder().appName(appName);
        for (Map.Entry<String, String> entry : sparkConf.entrySet()) {
            builder = builder.config(entry.getKey(), entry.getValue());
        }
        return builder.getOrCreate();
    }
}
