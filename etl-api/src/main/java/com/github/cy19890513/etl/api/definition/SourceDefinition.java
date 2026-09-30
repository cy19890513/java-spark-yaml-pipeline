package com.github.cy19890513.etl.api.definition;

/**
 * Definition of a source stage: reads one DataFrame from an external system.
 */
public class SourceDefinition extends StageDefinition {
    // No extra fields; everything lives in options (path, url, dbtable, ...).
}
