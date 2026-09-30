package com.github.cy19890513.etl.core;

/**
 * Thrown when a quality check fails with {@code FAIL} severity.
 * The pipeline stops immediately; no sink has written anything yet because
 * quality checks always run before sinks in the execution order.
 */
public class QualityCheckFailedException extends RuntimeException {

    private final String checkName;

    /**
     * Creates the exception.
     *
     * @param checkName name of the failed check
     * @param message   the check's detail message
     */
    public QualityCheckFailedException(String checkName, String message) {
        super("Quality check '" + checkName + "' failed: " + message);
        this.checkName = checkName;
    }

    /** Returns the name of the failed check. */
    public String checkName() {
        return checkName;
    }
}
