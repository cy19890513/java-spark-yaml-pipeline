package com.github.cy19890513.etl.api.definition;

import java.util.ArrayList;
import java.util.List;

/**
 * Root of a parsed pipeline: a name plus the four ordered stage groups.
 *
 * <p>Mirrors the YAML structure:
 * <pre>
 * name: daily-users
 * sources:
 *   - name: raw
 *     type: csv
 *     options:
 *       path: data/users.csv
 * transforms:
 *   - name: adults
 *     type: sql
 *     inputs: [raw]
 *     options:
 *       sql: "SELECT * FROM __input__ WHERE age &gt;= 18"
 * quality:
 *   - name: id-not-null
 *     type: not_null
 *     input: adults
 *     onFailure: FAIL
 *     options:
 *       columns: id
 * sinks:
 *   - name: out
 *     type: parquet
 *     input: adults
 *     options:
 *       path: out/users
 *       mode: overwrite
 * </pre>
 */
public class PipelineDefinition {

    private String name;
    private List<SourceDefinition> sources = new ArrayList<>();
    private List<TransformDefinition> transforms = new ArrayList<>();
    private List<QualityDefinition> quality = new ArrayList<>();
    private List<SinkDefinition> sinks = new ArrayList<>();

    /** Returns the pipeline name. */
    public String getName() {
        return name;
    }

    /** Sets the pipeline name. */
    public void setName(String name) {
        this.name = name;
    }

    /** Returns the source stage definitions. */
    public List<SourceDefinition> getSources() {
        return List.copyOf(sources);
    }

    /** Sets the source stage definitions. */
    public void setSources(List<SourceDefinition> sources) {
        this.sources = copyOf(sources);
    }

    /** Returns the transform stage definitions. */
    public List<TransformDefinition> getTransforms() {
        return List.copyOf(transforms);
    }

    /** Sets the transform stage definitions. */
    public void setTransforms(List<TransformDefinition> transforms) {
        this.transforms = copyOf(transforms);
    }

    /** Returns the quality-check definitions. */
    public List<QualityDefinition> getQuality() {
        return List.copyOf(quality);
    }

    /** Sets the quality-check definitions. */
    public void setQuality(List<QualityDefinition> quality) {
        this.quality = copyOf(quality);
    }

    /** Returns the sink stage definitions. */
    public List<SinkDefinition> getSinks() {
        return List.copyOf(sinks);
    }

    /** Sets the sink stage definitions. */
    public void setSinks(List<SinkDefinition> sinks) {
        this.sinks = copyOf(sinks);
    }

    private static <T> List<T> copyOf(List<T> list) {
        return list == null ? new ArrayList<>() : new ArrayList<>(list);
    }
}
