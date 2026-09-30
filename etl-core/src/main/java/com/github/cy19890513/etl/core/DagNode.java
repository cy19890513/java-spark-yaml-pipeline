package com.github.cy19890513.etl.core;

import com.github.cy19890513.etl.api.definition.StageDefinition;
import java.util.List;

/**
 * One executable unit of a pipeline, in dependency order.
 */
public final class DagNode {

    /** What kind of work the node performs. */
    public enum Kind {
        SOURCE,
        TRANSFORM,
        QUALITY_CHECK,
        SINK
    }

    private final Kind kind;
    private final StageDefinition definition;
    private final List<String> dependsOn;

    /**
     * Creates a DAG node.
     *
     * @param kind       the kind of work
     * @param definition the stage definition
     * @param dependsOn  upstream stage names this node reads, in order;
     *                   empty for sources
     */
    public DagNode(Kind kind, StageDefinition definition, List<String> dependsOn) {
        this.kind = kind;
        this.definition = definition;
        this.dependsOn = List.copyOf(dependsOn);
    }

    /** Returns the kind of work. */
    public Kind kind() {
        return kind;
    }

    /** Returns the stage definition. */
    public StageDefinition definition() {
        return definition;
    }

    /** Returns the upstream stage names, in order; empty for sources. */
    public List<String> dependsOn() {
        return dependsOn;
    }

    @Override
    public String toString() {
        return kind + "(" + definition.getName() + ")";
    }
}
