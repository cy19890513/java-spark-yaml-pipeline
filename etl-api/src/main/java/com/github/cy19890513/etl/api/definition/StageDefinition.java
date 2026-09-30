package com.github.cy19890513.etl.api.definition;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Base definition of a single pipeline stage, as parsed from YAML.
 *
 * <p>Subclasses add kind-specific fields (e.g. {@code input} for transforms,
 * {@code onFailure} for quality checks). Unknown YAML keys land in
 * {@link #options} so new stage types never require model changes.
 */
public class StageDefinition {

    private String name;
    private String type;
    private Map<String, String> options = new HashMap<>();

    /** Returns the logical stage name, unique within the pipeline. */
    public String getName() {
        return name;
    }

    /** Sets the logical stage name. */
    public void setName(String name) {
        this.name = name;
    }

    /** Returns the registered implementation type, e.g. {@code "csv"}. */
    public String getType() {
        return type;
    }

    /** Sets the registered implementation type. */
    public void setType(String type) {
        this.type = type;
    }

    /**
     * Returns the stage options (every YAML key not mapped to a typed field).
     *
     * @return unmodifiable view of the options, never null
     */
    public Map<String, String> getOptions() {
        return Collections.unmodifiableMap(options);
    }

    /** Sets the stage options. */
    public void setOptions(Map<String, String> options) {
        this.options = options == null ? new HashMap<>() : new HashMap<>(options);
    }
}
