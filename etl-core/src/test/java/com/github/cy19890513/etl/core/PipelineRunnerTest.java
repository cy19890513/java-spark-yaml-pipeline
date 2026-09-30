package com.github.cy19890513.etl.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.github.cy19890513.etl.api.QualityCheck;
import com.github.cy19890513.etl.api.QualityResult;
import com.github.cy19890513.etl.api.Severity;
import com.github.cy19890513.etl.api.SinkStage;
import com.github.cy19890513.etl.api.SourceStage;
import com.github.cy19890513.etl.api.StageRegistry;
import com.github.cy19890513.etl.api.TransformStage;
import com.github.cy19890513.etl.api.definition.PipelineDefinition;
import com.github.cy19890513.etl.api.definition.QualityDefinition;
import com.github.cy19890513.etl.api.definition.SinkDefinition;
import com.github.cy19890513.etl.api.definition.SourceDefinition;
import com.github.cy19890513.etl.api.definition.TransformDefinition;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * End-to-end {@link PipelineRunner} tests on a local Spark session with
 * stub stages, covering the happy path and the WARN/FAIL quality gates.
 */
class PipelineRunnerTest {

    private static SparkSession spark;
    private static StageRegistry registry;
    private static final AtomicLong sinkRows = new AtomicLong();
    private static final AtomicReference<QualityResult> nextResult = new AtomicReference<>();

    private static SourceStage stubSource() {
        return new SourceStage() {
            @Override
            public String name() {
                return "stub-source";
            }

            @Override
            public String type() {
                return "stub";
            }

            @Override
            public Dataset<Row> read(SparkSession session, Map<String, String> options) {
                return session.range(5).toDF("id");
            }
        };
    }

    private static TransformStage stubTransform() {
        return new TransformStage() {
            @Override
            public String name() {
                return "stub-transform";
            }

            @Override
            public String type() {
                return "stub";
            }

            @Override
            public Dataset<Row> transform(Map<String, Dataset<Row>> inputs,
                    Map<String, String> options) {
                Dataset<Row> input = inputs.values().iterator().next();
                return input.filter("id > 1");
            }
        };
    }

    private static QualityCheck stubCheck() {
        return new QualityCheck() {
            @Override
            public String name() {
                return "stub-check";
            }

            @Override
            public String type() {
                return "stub";
            }

            @Override
            public QualityResult check(Dataset<Row> input, Map<String, String> options) {
                return nextResult.get();
            }
        };
    }

    private static SinkStage stubSink() {
        return new SinkStage() {
            @Override
            public String name() {
                return "stub-sink";
            }

            @Override
            public String type() {
                return "stub";
            }

            @Override
            public void write(Dataset<Row> input, Map<String, String> options) {
                sinkRows.set(input.count());
            }
        };
    }

    @BeforeAll
    static void startSpark() {
        spark = SparkSession.builder()
                .appName("pipeline-runner-test")
                .master("local[2]")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.shuffle.partitions", "2")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        registry = new StageRegistry();
        registry.registerSource("stub", PipelineRunnerTest::stubSource);
        registry.registerTransform("stub", PipelineRunnerTest::stubTransform);
        registry.registerQualityCheck("stub", PipelineRunnerTest::stubCheck);
        registry.registerSink("stub", PipelineRunnerTest::stubSink);
    }

    @AfterAll
    static void stopSpark() {
        if (spark != null) {
            spark.stop();
        }
    }

    @BeforeEach
    void reset() {
        sinkRows.set(-1);
        nextResult.set(QualityResult.pass("stub", "ok"));
    }

    private static PipelineDefinition pipeline(Severity onFailure) {
        PipelineDefinition def = new PipelineDefinition();
        def.setName("test");

        SourceDefinition source = new SourceDefinition();
        source.setName("raw");
        source.setType("stub");
        def.setSources(List.of(source));

        TransformDefinition transform = new TransformDefinition();
        transform.setName("filtered");
        transform.setType("stub");
        transform.setInputs(List.of("raw"));
        def.setTransforms(List.of(transform));

        QualityDefinition check = new QualityDefinition();
        check.setName("check");
        check.setType("stub");
        check.setInput("filtered");
        check.setOnFailure(onFailure);
        def.setQuality(List.of(check));

        SinkDefinition sink = new SinkDefinition();
        sink.setName("out");
        sink.setType("stub");
        sink.setInput("filtered");
        def.setSinks(List.of(sink));

        return def;
    }

    @Test
    void happyPathRunsAllStages() {
        new PipelineRunner(registry, spark).run(pipeline(null));

        // ids 2, 3, 4 survive the "id > 1" filter.
        assertEquals(3, sinkRows.get());
    }

    @Test
    void failedCheckWithFailSeverityAbortsBeforeSink() {
        nextResult.set(QualityResult.fail("stub", "bad data", Severity.FAIL));

        assertThrows(QualityCheckFailedException.class,
                () -> new PipelineRunner(registry, spark).run(pipeline(null)));
        assertEquals(-1, sinkRows.get());
    }

    @Test
    void failedCheckWithWarnSeverityContinues() {
        nextResult.set(QualityResult.fail("stub", "bad data", Severity.WARN));

        new PipelineRunner(registry, spark).run(pipeline(null));

        assertEquals(3, sinkRows.get());
    }

    @Test
    void yamlOnFailureOverridesCheckSeverity() {
        nextResult.set(QualityResult.fail("stub", "bad data", Severity.WARN));

        assertThrows(QualityCheckFailedException.class,
                () -> new PipelineRunner(registry, spark).run(pipeline(Severity.FAIL)));
        assertEquals(-1, sinkRows.get());
    }
}
