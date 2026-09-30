package com.github.cy19890513.etl.core;

import java.util.List;

/**
 * Thrown when a pipeline definition fails structural validation.
 * Carries every problem found so callers can report them all at once.
 */
public class PipelineValidationException extends RuntimeException {

    private final List<String> errors;

    /**
     * Creates the exception with the given validation errors.
     *
     * @param errors human-readable error descriptions, never null or empty
     */
    public PipelineValidationException(List<String> errors) {
        super("Invalid pipeline definition:\n  - " + String.join("\n  - ", errors));
        this.errors = List.copyOf(errors);
    }

    /**
     * Returns the individual validation errors.
     *
     * @return unmodifiable list of error descriptions
     */
    public List<String> errors() {
        return errors;
    }
}
