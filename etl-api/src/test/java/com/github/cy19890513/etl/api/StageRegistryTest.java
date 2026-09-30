package com.github.cy19890513.etl.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link StageRegistry}.
 */
class StageRegistryTest {

    private static SourceStage stubSource() {
        return new SourceStage() {
            @Override
            public String name() {
                return "stub";
            }

            @Override
            public String type() {
                return "stub";
            }

            @Override
            public org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> read(
                    org.apache.spark.sql.SparkSession spark, java.util.Map<String, String> options) {
                return null;
            }
        };
    }

    @Test
    void registeredSourceIsCreated() {
        StageRegistry registry = new StageRegistry();
        registry.registerSource("csv", StageRegistryTest::stubSource);

        SourceStage stage = registry.createSource("csv");

        assertTrue(stage instanceof SourceStage);
        assertTrue(registry.sourceTypes().contains("csv"));
    }

    @Test
    void unknownTypeThrows() {
        StageRegistry registry = new StageRegistry();

        assertThrows(UnknownStageTypeException.class, () -> registry.createSource("nope"));
        assertThrows(UnknownStageTypeException.class, () -> registry.createTransform("nope"));
        assertThrows(UnknownStageTypeException.class, () -> registry.createQualityCheck("nope"));
        assertThrows(UnknownStageTypeException.class, () -> registry.createSink("nope"));
    }

    @Test
    void blankTypeIsRejected() {
        StageRegistry registry = new StageRegistry();

        assertThrows(IllegalArgumentException.class,
                () -> registry.registerSource("  ", StageRegistryTest::stubSource));
    }

    @Test
    void qualityResultFactories() {
        QualityResult pass = QualityResult.pass("not_null", "0 nulls in [id]");
        assertTrue(pass.passed());
        assertEquals("not_null", pass.checkName());

        QualityResult fail = QualityResult.fail("not_null", "3 nulls in [id]", Severity.FAIL);
        assertTrue(!fail.passed());
        assertEquals(Severity.FAIL, fail.severity());
    }
}
