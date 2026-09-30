package com.github.cy19890513.etl.api.definition;

import com.github.cy19890513.etl.api.Severity;

/**
 * Definition of a quality-check stage: validates one upstream DataFrame.
 */
public class QualityDefinition extends StageDefinition {

    private String input;
    private Severity onFailure;

    /** Returns the upstream stage name this check validates. */
    public String getInput() {
        return input;
    }

    /** Sets the upstream stage name this check validates. */
    public void setInput(String input) {
        this.input = input;
    }

    /**
     * Returns the user-declared failure policy, or null when the YAML did not
     * set {@code on_failure}. The runner falls back to the check result's own
     * severity in that case.
     *
     * @return the declared policy, or null when unset
     */
    public Severity getOnFailure() {
        return onFailure;
    }

    /** Sets the failure policy; null means "use the check's own severity". */
    public void setOnFailure(Severity onFailure) {
        this.onFailure = onFailure;
    }
}
