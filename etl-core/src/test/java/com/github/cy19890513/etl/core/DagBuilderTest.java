package com.github.cy19890513.etl.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.cy19890513.etl.api.definition.PipelineDefinition;
import com.github.cy19890513.etl.api.definition.QualityDefinition;
import com.github.cy19890513.etl.api.definition.SinkDefinition;
import com.github.cy19890513.etl.api.definition.SourceDefinition;
import com.github.cy19890513.etl.api.definition.TransformDefinition;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link DagBuilder}: topological ordering and cycle detection.
 */
class DagBuilderTest {

    private static PipelineDefinition linearPipeline() {
        PipelineDefinition def = new PipelineDefinition();
        def.setName("linear");

        SourceDefinition source = new SourceDefinition();
        source.setName("raw");
        source.setType("csv");
        def.setSources(List.of(source));

        TransformDefinition transform = new TransformDefinition();
        transform.setName("cleaned");
        transform.setType("sql");
        transform.setInputs(List.of("raw"));
        def.setTransforms(List.of(transform));

        QualityDefinition check = new QualityDefinition();
        check.setName("check");
        check.setType("not_null");
        check.setInput("cleaned");
        def.setQuality(List.of(check));

        SinkDefinition sink = new SinkDefinition();
        sink.setName("out");
        sink.setType("parquet");
        sink.setInput("cleaned");
        def.setSinks(List.of(sink));

        return def;
    }

    private static List<String> orderOf(PipelineDag dag) {
        return dag.executionOrder().stream()
                .map(n -> n.definition().getName())
                .collect(Collectors.toList());
    }

    @Test
    void linearPipelineKeepsDeclarationOrder() {
        PipelineDag dag = DagBuilder.build(linearPipeline());

        assertEquals(List.of("raw", "cleaned", "check", "out"), orderOf(dag));
    }

    @Test
    void diamondDependenciesAreOrdered() {
        PipelineDefinition def = linearPipeline();

        TransformDefinition left = new TransformDefinition();
        left.setName("left");
        left.setType("sql");
        left.setInputs(List.of("raw"));

        TransformDefinition right = new TransformDefinition();
        right.setName("right");
        right.setType("sql");
        right.setInputs(List.of("raw"));

        TransformDefinition joined = new TransformDefinition();
        joined.setName("joined");
        joined.setType("sql");
        joined.setInputs(List.of("left", "right"));

        def.setTransforms(List.of(
                def.getTransforms().get(0), left, right, joined));
        // 'cleaned' now feeds nothing new; point the check and sink at 'joined'.
        def.getQuality().get(0).setInput("joined");
        def.getSinks().get(0).setInput("joined");

        List<String> order = orderOf(DagBuilder.build(def));

        assertTrue(order.indexOf("raw") < order.indexOf("left"));
        assertTrue(order.indexOf("raw") < order.indexOf("right"));
        assertTrue(order.indexOf("left") < order.indexOf("joined"));
        assertTrue(order.indexOf("right") < order.indexOf("joined"));
        assertTrue(order.indexOf("joined") < order.indexOf("check"));
        assertTrue(order.indexOf("joined") < order.indexOf("out"));
    }

    @Test
    void cycleIsRejected() {
        PipelineDefinition def = linearPipeline();

        TransformDefinition first = new TransformDefinition();
        first.setName("first");
        first.setType("sql");
        first.setInputs(List.of("second"));

        TransformDefinition second = new TransformDefinition();
        second.setName("second");
        second.setType("sql");
        second.setInputs(List.of("first"));

        def.setTransforms(List.of(first, second));
        def.setQuality(List.of());
        // Sink reads 'second' so the definition is otherwise valid.
        def.getSinks().get(0).setInput("second");

        PipelineValidationException ex = assertThrows(
                PipelineValidationException.class, () -> DagBuilder.build(def));

        assertTrue(ex.errors().get(0).contains("cycle"));
    }
}
