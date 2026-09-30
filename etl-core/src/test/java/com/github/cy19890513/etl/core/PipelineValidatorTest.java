package com.github.cy19890513.etl.core;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.cy19890513.etl.api.definition.PipelineDefinition;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link PipelineValidator}: structural validation rules.
 */
class PipelineValidatorTest {

    private static PipelineDefinition load(String name) throws Exception {
        Path path = Paths.get("src", "test", "resources", name);
        return new PipelineLoader().load(path);
    }

    @Test
    void validPipelinePasses() throws Exception {
        assertDoesNotThrow(() -> PipelineValidator.validate(load("valid-pipeline.yaml")));
    }

    @Test
    void duplicateStageNamesFail() throws Exception {
        PipelineValidationException ex = assertThrows(PipelineValidationException.class,
                () -> PipelineValidator.validate(load("duplicate-names.yaml")));

        assertTrue(ex.errors().stream().anyMatch(e -> e.contains("Duplicate stage name")));
    }

    @Test
    void selfReferenceFails() throws Exception {
        // duplicate-names.yaml also has transform 'raw' consuming input 'raw'.
        PipelineValidationException ex = assertThrows(PipelineValidationException.class,
                () -> PipelineValidator.validate(load("duplicate-names.yaml")));

        assertTrue(ex.errors().stream().anyMatch(e -> e.contains("own output")));
    }

    @Test
    void danglingInputFails() throws Exception {
        PipelineValidationException ex = assertThrows(PipelineValidationException.class,
                () -> PipelineValidator.validate(load("dangling-input.yaml")));

        assertTrue(ex.errors().stream().anyMatch(e -> e.contains("unknown input 'ghost'")));
    }

    @Test
    void missingPipelineNameFails() throws Exception {
        PipelineValidationException ex = assertThrows(PipelineValidationException.class,
                () -> PipelineValidator.validate(load("missing-name.yaml")));

        assertTrue(ex.errors().stream().anyMatch(e -> e.contains("'name' is required")));
    }

    @Test
    void nullDefinitionFails() {
        assertThrows(PipelineValidationException.class,
                () -> PipelineValidator.validate(null));
    }

    @Test
    void qualityCheckCannotConsumeSinkOutput() throws Exception {
        PipelineDefinition def = load("valid-pipeline.yaml");
        // Point the quality check at the sink 'gold_events', which produces no DataFrame.
        def.getQuality().get(0).setInput("gold_events");

        PipelineValidationException ex = assertThrows(PipelineValidationException.class,
                () -> PipelineValidator.validate(def));

        assertTrue(ex.errors().stream().anyMatch(e -> e.contains("does not produce a DataFrame")));
    }
}
