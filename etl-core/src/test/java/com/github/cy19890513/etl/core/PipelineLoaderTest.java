package com.github.cy19890513.etl.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.cy19890513.etl.api.Severity;
import com.github.cy19890513.etl.api.definition.PipelineDefinition;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link PipelineLoader}: YAML conventions and option capture.
 */
class PipelineLoaderTest {

    private static Path resource(String name) {
        return Paths.get("src", "test", "resources", name);
    }

    @Test
    void loadsValidPipeline() throws Exception {
        PipelineDefinition def = new PipelineLoader().load(resource("valid-pipeline.yaml"));

        assertEquals("user-events-daily", def.getName());
        assertEquals(1, def.getSources().size());
        assertEquals("raw_events", def.getSources().get(0).getName());
        assertEquals("csv", def.getSources().get(0).getType());
    }

    @Test
    void nestedOptionsAreCaptured() throws Exception {
        PipelineDefinition def = new PipelineLoader().load(resource("valid-pipeline.yaml"));

        assertEquals("data/events.csv", def.getSources().get(0).getOptions().get("path"));
        assertEquals("true", def.getSources().get(0).getOptions().get("header"));
    }

    @Test
    void flatKeysAreCapturedAsOptions() throws Exception {
        PipelineDefinition def = new PipelineLoader().load(resource("valid-pipeline.yaml"));

        // 'sql' is not a typed field, so it lands in options.
        assertTrue(def.getTransforms().get(0).getOptions().get("sql").contains("user_id"));
        assertEquals("./data/output/events", def.getSinks().get(0).getOptions().get("path"));
        assertEquals("overwrite", def.getSinks().get(0).getOptions().get("mode"));
    }

    @Test
    void singularInputBecomesInputsList() throws Exception {
        PipelineDefinition def = new PipelineLoader().load(resource("valid-pipeline.yaml"));

        assertEquals(java.util.List.of("raw_events"), def.getTransforms().get(0).getInputs());
    }

    @Test
    void onFailureIsCaseInsensitive() throws Exception {
        PipelineDefinition def = new PipelineLoader().load(resource("valid-pipeline.yaml"));

        assertEquals(Severity.FAIL, def.getQuality().get(0).getOnFailure());
    }

    @Test
    void yamlListOptionIsCoercedToString() throws Exception {
        PipelineDefinition def = new PipelineLoader().load(resource("valid-pipeline.yaml"));

        assertEquals("[user_id]", def.getQuality().get(0).getOptions().get("columns"));
    }

    @Test
    void missingFileThrows() {
        assertThrows(java.io.IOException.class,
                () -> new PipelineLoader().load(resource("does-not-exist.yaml")));
    }
}
