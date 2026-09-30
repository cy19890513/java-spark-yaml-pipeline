package com.github.cy19890513.etl.core;

import java.util.List;

/**
 * A validated pipeline arranged in dependency order.
 */
public final class PipelineDag {

    private final List<DagNode> executionOrder;

    /**
     * Creates the DAG.
     *
     * @param executionOrder nodes in the order they must run; every node
     *                       appears after all of its dependencies
     */
    public PipelineDag(List<DagNode> executionOrder) {
        this.executionOrder = List.copyOf(executionOrder);
    }

    /**
     * Returns the nodes in execution order.
     *
     * @return unmodifiable list, dependencies before dependents
     */
    public List<DagNode> executionOrder() {
        return executionOrder;
    }
}
