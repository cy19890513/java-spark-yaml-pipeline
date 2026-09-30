package com.github.cy19890513.etl.api.definition;

import com.github.cy19890513.etl.api.Severity;

/**
 * Definition of a quality-check stage: validates one upstream DataFrame.
 */
public class QualityDefinition extends StageDefinition {

    private String input;
    private Severity onFailure = Severity.FAIL;

    /** Returns the upstream stage name this check validates. */
    public String getInput() {
        return input;
    }

    /** Sets the upstream stage name this check validates. */
    public void setInput(String input) {
        this.input = input;
    }

    /**
     * Returns what happens when the check fails; defaults to {@link Severity#FAIL}.
     */
    public Severity getOnFailure() {
        return onFailure;
    }

    /** Sets what happens when the check fails. */
    public void setOnFailure(Severity onFailure) {
        this.onFailure = onFailure == null ? Severity.FAIL : onFailure;
    }
}
