package com.github.cy19890513.etl.api;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Registry that maps stage type names to their implementations.
 *
 * <p>Connectors and quality-check modules register themselves here (usually
 * from a {@code ServiceLoader} or an explicit bootstrap class); the pipeline
 * runner resolves types from the YAML definition through this registry.
 *
 * <p>Instances are thread-safe for registration and lookup.
 */
public final class StageRegistry {

    private final Map<String, Supplier<SourceStage>> sources = new HashMap<>();
    private final Map<String, Supplier<TransformStage>> transforms = new HashMap<>();
    private final Map<String, Supplier<QualityCheck>> qualityChecks = new HashMap<>();
    private final Map<String, Supplier<SinkStage>> sinks = new HashMap<>();

    /**
     * Registers a source implementation under the given type name.
     *
     * @param type    type name used in pipeline definitions, e.g. {@code "csv"}
     * @param factory supplier creating a fresh instance per use
     */
    public synchronized void registerSource(String type, Supplier<SourceStage> factory) {
        sources.put(requireType(type), factory);
    }

    /**
     * Registers a transform implementation under the given type name.
     *
     * @param type    type name used in pipeline definitions, e.g. {@code "sql"}
     * @param factory supplier creating a fresh instance per use
     */
    public synchronized void registerTransform(String type, Supplier<TransformStage> factory) {
        transforms.put(requireType(type), factory);
    }

    /**
     * Registers a quality-check implementation under the given type name.
     *
     * @param type    type name used in pipeline definitions, e.g. {@code "not_null"}
     * @param factory supplier creating a fresh instance per use
     */
    public synchronized void registerQualityCheck(String type, Supplier<QualityCheck> factory) {
        qualityChecks.put(requireType(type), factory);
    }

    /**
     * Registers a sink implementation under the given type name.
     *
     * @param type    type name used in pipeline definitions, e.g. {@code "parquet"}
     * @param factory supplier creating a fresh instance per use
     */
    public synchronized void registerSink(String type, Supplier<SinkStage> factory) {
        sinks.put(requireType(type), factory);
    }

    /**
     * Creates a source instance for the given type.
     *
     * @param type registered type name
     * @return a fresh source instance
     * @throws UnknownStageTypeException when the type was never registered
     */
    public synchronized SourceStage createSource(String type) {
        Supplier<SourceStage> factory = sources.get(type);
        if (factory == null) {
            throw new UnknownStageTypeException("source", type);
        }
        return factory.get();
    }

    /**
     * Creates a transform instance for the given type.
     *
     * @param type registered type name
     * @return a fresh transform instance
     * @throws UnknownStageTypeException when the type was never registered
     */
    public synchronized TransformStage createTransform(String type) {
        Supplier<TransformStage> factory = transforms.get(type);
        if (factory == null) {
            throw new UnknownStageTypeException("transform", type);
        }
        return factory.get();
    }

    /**
     * Creates a quality-check instance for the given type.
     *
     * @param type registered type name
     * @return a fresh check instance
     * @throws UnknownStageTypeException when the type was never registered
     */
    public synchronized QualityCheck createQualityCheck(String type) {
        Supplier<QualityCheck> factory = qualityChecks.get(type);
        if (factory == null) {
            throw new UnknownStageTypeException("quality check", type);
        }
        return factory.get();
    }

    /**
     * Creates a sink instance for the given type.
     *
     * @param type registered type name
     * @return a fresh sink instance
     * @throws UnknownStageTypeException when the type was never registered
     */
    public synchronized SinkStage createSink(String type) {
        Supplier<SinkStage> factory = sinks.get(type);
        if (factory == null) {
            throw new UnknownStageTypeException("sink", type);
        }
        return factory.get();
    }

    /** Returns the registered source type names. */
    public synchronized Set<String> sourceTypes() {
        return Set.copyOf(sources.keySet());
    }

    /** Returns the registered transform type names. */
    public synchronized Set<String> transformTypes() {
        return Set.copyOf(transforms.keySet());
    }

    /** Returns the registered quality-check type names. */
    public synchronized Set<String> qualityCheckTypes() {
        return Set.copyOf(qualityChecks.keySet());
    }

    /** Returns the registered sink type names. */
    public synchronized Set<String> sinkTypes() {
        return Set.copyOf(sinks.keySet());
    }

    private static String requireType(String type) {
        if (type == null || type.isBlank()) {
            throw new IllegalArgumentException("Stage type must be a non-blank string");
        }
        return type;
    }
}
