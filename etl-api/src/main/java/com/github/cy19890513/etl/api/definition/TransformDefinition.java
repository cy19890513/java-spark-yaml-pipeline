package com.github.cy19890513.etl.api.definition;

import com.fasterxml.jackson.annotation.JsonSetter;
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

    /**
     * Accepts the singular {@code input:} YAML key as shorthand for a
     * one-element {@code inputs} list.
     *
     * @param input single upstream stage name
     */
    @JsonSetter("input")
    public void setInput(String input) {
        this.inputs = input == null ? new ArrayList<>() : new ArrayList<>(List.of(input));
    }
}
