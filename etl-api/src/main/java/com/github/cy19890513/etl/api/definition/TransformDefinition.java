package com.github.cy19890513.etl.api.definition;

import java.util.ArrayList;
import java.util.List;

/**
 * Definition of a transform stage: derives a DataFrame from upstream stages.
 */
public class TransformDefinition extends StageDefinition {

    private List<String> inputs = new ArrayList<>();

    /**
     * Returns the upstream stage names feeding this transform, in order.
     *
     * @return unmodifiable list, never null
     */
    public List<String> getInputs() {
        return List.copyOf(inputs);
    }

    /** Sets the upstream stage names. */
    public void setInputs(List<String> inputs) {
        this.inputs = inputs == null ? new ArrayList<>() : new ArrayList<>(inputs);
    }
}
