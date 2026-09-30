package com.github.cy19890513.etl.api;

import java.util.Objects;

/**
 * Immutable outcome of a single {@link QualityCheck} evaluation.
 */
public final class QualityResult {

    private final String checkName;
    private final boolean passed;
    private final String message;
    private final Severity severity;

    private QualityResult(String checkName, boolean passed, String message, Severity severity) {
        this.checkName = checkName;
        this.passed = passed;
        this.message = message;
        this.severity = severity;
    }

    /**
     * Creates a passing result.
     *
     * @param checkName name of the check that ran
     * @param message  human-readable detail, e.g. {@code "0 nulls in [id]"}
     * @return passing result
     */
    public static QualityResult pass(String checkName, String message) {
        return new QualityResult(checkName, true, message, Severity.WARN);
    }

    /**
     * Creates a failing result.
     *
     * @param checkName name of the check that ran
     * @param message   human-readable detail, e.g. {@code "42 nulls in [email]"}
     * @param severity  what the runner should do about the failure
     * @return failing result
     */
    public static QualityResult fail(String checkName, String message, Severity severity) {
        return new QualityResult(checkName, false, message, severity);
    }

    /** Returns the name of the check that produced this result. */
    public String checkName() {
        return checkName;
    }

    /** Returns true when the check passed. */
    public boolean passed() {
        return passed;
    }

    /** Returns the human-readable detail message. */
    public String message() {
        return message;
    }

    /** Returns the severity attached to a failing result. */
    public Severity severity() {
        return severity;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof QualityResult)) return false;
        QualityResult that = (QualityResult) o;
        return passed == that.passed
                && Objects.equals(checkName, that.checkName)
                && Objects.equals(message, that.message)
                && severity == that.severity;
    }

    @Override
    public int hashCode() {
        return Objects.hash(checkName, passed, message, severity);
    }

    @Override
    public String toString() {
        return (passed ? "PASS" : "FAIL[" + severity + "]") + " " + checkName + ": " + message;
    }
}
