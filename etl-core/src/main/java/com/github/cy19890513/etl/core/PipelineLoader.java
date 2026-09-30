package com.github.cy19890513.etl.core;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.github.cy19890513.etl.api.definition.PipelineDefinition;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Parses pipeline YAML files into {@link PipelineDefinition} objects.
 *
 * <p>Accepted YAML conventions:
 * <ul>
 *   <li>snake_case keys ({@code on_failure}) map to camelCase fields;</li>
 *   <li>enum values are case-insensitive ({@code fail} means {@code FAIL});</li>
 *   <li>a singular {@code input:} key is shorthand for a one-element
 *       {@code inputs} list on transforms;</li>
 *   <li>any other key on a stage (e.g. {@code path}, {@code sql},
 *       {@code mode}) is collected into the stage options, either nested
 *       under {@code options:} or flat next to the typed keys.</li>
 * </ul>
 */
public final class PipelineLoader {

    private final ObjectMapper mapper;

    /** Creates a loader with the framework's YAML conventions. */
    public PipelineLoader() {
        mapper = new ObjectMapper(new YAMLFactory());
        mapper.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
        mapper.enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS);
        mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    }

    /**
     * Loads and parses a pipeline YAML file.
     *
     * @param yamlFile path to the YAML file
     * @return the parsed pipeline definition, never null
     * @throws IOException when the file cannot be read or parsed
     */
    public PipelineDefinition load(Path yamlFile) throws IOException {
        try (InputStream in = Files.newInputStream(yamlFile)) {
            PipelineDefinition definition = mapper.readValue(in, PipelineDefinition.class);
            if (definition == null) {
                throw new IOException("Empty pipeline definition: " + yamlFile);
            }
            return definition;
        }
    }
}
