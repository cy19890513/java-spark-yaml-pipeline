package com.github.cy19890513.etl.core;

import com.github.cy19890513.etl.api.QualityCheck;
import com.github.cy19890513.etl.api.QualityResult;
import com.github.cy19890513.etl.api.Severity;
import com.github.cy19890513.etl.api.SinkStage;
import com.github.cy19890513.etl.api.SourceStage;
import com.github.cy19890513.etl.api.StageRegistry;
import com.github.cy19890513.etl.api.TransformStage;
import com.github.cy19890513.etl.api.definition.PipelineDefinition;
import com.github.cy19890513.etl.api.definition.QualityDefinition;
import com.github.cy19890513.etl.api.definition.StageDefinition;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executes a pipeline definition on Spark.
 *
 * <p>Execution order comes from {@link DagBuilder} (dependencies first).
 * Quality checks run before any sink, so a {@code FAIL}-severity failure
 * aborts the run via {@link QualityCheckFailedException} before anything is
 * written. A {@code WARN}-severity failure is logged and the run continues.
 *
 * <p>Which severity applies: the YAML {@code on_failure} policy when set,
 * otherwise the failing check's own {@link QualityResult#severity()}.
 */
public final class PipelineRunner {

    private static final Logger log = LoggerFactory.getLogger(PipelineRunner.class);

    private final StageRegistry registry;
    private final SparkSession borrowedSession;

    /**
     * Creates a runner that opens and closes its own Spark session per run.
     *
     * @param registry stage implementations to run
     */
    public PipelineRunner(StageRegistry registry) {
        this(registry, null);
    }

    /**
     * Creates a runner that borrows an existing Spark session (used by tests).
     * The borrowed session is never stopped by the runner.
     *
     * @param registry stage implementations to run
     * @param session  session to run on, never stopped by this runner
     */
    public PipelineRunner(StageRegistry registry, SparkSession session) {
        this.registry = registry;
        this.borrowedSession = session;
    }

    /**
     * Validates the definition, builds the DAG, and runs every stage.
     *
     * @param definition the pipeline to run
     * @throws PipelineValidationException  when the definition is invalid
     * @throws QualityCheckFailedException  when a check fails with FAIL severity
     */
    public void run(PipelineDefinition definition) {
        PipelineDag dag = DagBuilder.build(definition);

        SparkSession spark = borrowedSession != null
                ? borrowedSession
                : SparkSessionFactory.create("etl-" + definition.getName());
        boolean ownsSession = borrowedSession == null;
        try {
            Map<String, Dataset<Row>> frames = new HashMap<>();
            for (DagNode node : dag.executionOrder()) {
                execute(node, spark, frames);
            }
            log.info("Pipeline '{}' completed", definition.getName());
        } finally {
            if (ownsSession) {
                spark.stop();
            }
        }
    }

    private void execute(DagNode node, SparkSession spark, Map<String, Dataset<Row>> frames) {
        StageDefinition def = node.definition();
        log.info("Running {} '{}' (type '{}')", node.kind(), def.getName(), def.getType());
        switch (node.kind()) {
            case SOURCE -> {
                SourceStage stage = registry.createSource(def.getType());
                frames.put(def.getName(), stage.read(spark, def.getOptions()));
            }
            case TRANSFORM -> {
                TransformStage stage = registry.createTransform(def.getType());
                Map<String, Dataset<Row>> inputs = new LinkedHashMap<>();
                for (String upstream : node.dependsOn()) {
                    inputs.put(upstream, frames.get(upstream));
                }
                frames.put(def.getName(), stage.transform(inputs, def.getOptions()));
            }
            case QUALITY_CHECK -> runCheck((QualityDefinition) def, frames);
            case SINK -> {
                SinkStage stage = registry.createSink(def.getType());
                stage.write(frames.get(node.dependsOn().get(0)), def.getOptions());
            }
        }
    }

    private void runCheck(QualityDefinition def, Map<String, Dataset<Row>> frames) {
        QualityCheck check = registry.createQualityCheck(def.getType());
        Dataset<Row> input = frames.get(def.getInput());
        QualityResult result = check.check(input, def.getOptions());
        if (result.passed()) {
            log.info("Quality check '{}' passed: {}", def.getName(), result.message());
            return;
        }
        Severity severity = def.getOnFailure() != null ? def.getOnFailure() : result.severity();
        if (severity == Severity.FAIL) {
            throw new QualityCheckFailedException(def.getName(), result.message());
        }
        log.warn("Quality check '{}' failed but continues (WARN): {}",
                def.getName(), result.message());
    }
}
