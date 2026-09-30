package com.github.cy19890513.etl.api;

/**
 * Thrown when a pipeline references a stage type that no implementation
 * was registered for.
 */
public class UnknownStageTypeException extends RuntimeException {

    /**
     * Creates the exception for the given kind and type.
     *
     * @param kind human-readable stage kind, e.g. {@code "source"}
     * @param type the unregistered type name from the pipeline definition
     */
    public UnknownStageTypeException(String kind, String type) {
        super("Unknown " + kind + " type: '" + type + "'. "
                + "Register an implementation with StageRegistry first.");
    }
}
