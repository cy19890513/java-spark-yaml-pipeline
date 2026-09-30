package com.github.cy19890513.etl.core;

import com.github.cy19890513.etl.api.definition.PipelineDefinition;
import com.github.cy19890513.etl.api.definition.QualityDefinition;
import com.github.cy19890513.etl.api.definition.SinkDefinition;
import com.github.cy19890513.etl.api.definition.SourceDefinition;
import com.github.cy19890513.etl.api.definition.TransformDefinition;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * Builds an executable {@link PipelineDag} from a pipeline definition.
 *
 * <p>Stages are topologically sorted (Kahn's algorithm) so every stage runs
 * after the stages it reads from. A dependency cycle is a structural error
 * and fails the build.
 */
public final class DagBuilder {

    private DagBuilder() {
        // utility class
    }

    /**
     * Validates the definition and builds its execution DAG.
     *
     * @param definition the pipeline definition
     * @return the DAG in execution order
     * @throws PipelineValidationException when the definition is invalid or
     *         contains a dependency cycle
     */
    public static PipelineDag build(PipelineDefinition definition) {
        PipelineValidator.validate(definition);

        List<DagNode> nodes = new ArrayList<>();
        for (SourceDefinition source : definition.getSources()) {
            nodes.add(new DagNode(DagNode.Kind.SOURCE, source, List.of()));
        }
        for (TransformDefinition transform : definition.getTransforms()) {
            nodes.add(new DagNode(DagNode.Kind.TRANSFORM, transform, transform.getInputs()));
        }
        for (QualityDefinition check : definition.getQuality()) {
            nodes.add(new DagNode(DagNode.Kind.QUALITY_CHECK, check, List.of(check.getInput())));
        }
        for (SinkDefinition sink : definition.getSinks()) {
            nodes.add(new DagNode(DagNode.Kind.SINK, sink, List.of(sink.getInput())));
        }

        return new PipelineDag(topologicalSort(nodes));
    }

    private static List<DagNode> topologicalSort(List<DagNode> nodes) {
        Map<String, DagNode> byName = new HashMap<>();
        Map<String, Integer> inDegree = new HashMap<>();
        Map<String, List<String>> dependents = new HashMap<>();

        for (DagNode node : nodes) {
            String name = node.definition().getName();
            byName.put(name, node);
            inDegree.put(name, node.dependsOn().size());
            dependents.put(name, new ArrayList<>());
        }
        for (DagNode node : nodes) {
            for (String dep : node.dependsOn()) {
                dependents.get(dep).add(node.definition().getName());
            }
        }

        Deque<String> ready = new ArrayDeque<>();
        for (Map.Entry<String, Integer> entry : inDegree.entrySet()) {
            if (entry.getValue() == 0) {
                ready.add(entry.getKey());
            }
        }

        List<DagNode> ordered = new ArrayList<>();
        while (!ready.isEmpty()) {
            String name = ready.removeFirst();
            ordered.add(byName.get(name));
            for (String dependent : dependents.get(name)) {
                int remaining = inDegree.get(dependent) - 1;
                inDegree.put(dependent, remaining);
                if (remaining == 0) {
                    ready.add(dependent);
                }
            }
        }

        if (ordered.size() != nodes.size()) {
            TreeSet<String> stuck = new TreeSet<>(byName.keySet());
            for (DagNode node : ordered) {
                stuck.remove(node.definition().getName());
            }
            throw new PipelineValidationException(
                    List.of("Dependency cycle detected involving stages: " + stuck));
        }
        return ordered;
    }
}
