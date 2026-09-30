package com.github.cy19890513.etl.api.definition;

/**
 * Definition of a sink stage: writes one upstream DataFrame out.
 */
public class SinkDefinition extends StageDefinition {

    private String input;

    /** Returns the upstream stage name this sink writes. */
    public String getInput() {
        return input;
    }

    /** Sets the upstream stage name this sink writes. */
    public void setInput(String input) {
        this.input = input;
    }
}
