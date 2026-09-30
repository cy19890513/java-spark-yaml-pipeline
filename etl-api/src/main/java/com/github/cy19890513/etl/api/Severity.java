package com.github.cy19890513.etl.api;

/**
 * What the pipeline should do when a {@link QualityCheck} fails.
 */
public enum Severity {

    /**
     * Log the failure and keep the pipeline running.
     */
    WARN,

    /**
     * Abort the pipeline immediately; nothing downstream runs and no sink
     * writes anything.
     */
    FAIL
}
