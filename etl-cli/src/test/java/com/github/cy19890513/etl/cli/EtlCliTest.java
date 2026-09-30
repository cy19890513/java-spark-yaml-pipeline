package com.github.cy19890513.etl.cli;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * Tests for {@link EtlCli.Arguments} parsing (no Spark needed).
 */
class EtlCliTest {

    @Test
    void parsesPipelineAndOptions() {
        EtlCli.Arguments args = EtlCli.Arguments.parse(new String[] {
                "pipeline.yaml",
                "--master", "spark://cluster:7077",
                "--conf", "spark.executor.memory=4g",
                "--conf", "spark.sql.shuffle.partitions=8",
        });

        assertEquals("pipeline.yaml", args.pipeline.toString());
        assertEquals("spark://cluster:7077", args.sparkConf.get("spark.master"));
        assertEquals("4g", args.sparkConf.get("spark.executor.memory"));
        assertEquals("8", args.sparkConf.get("spark.sql.shuffle.partitions"));
    }

    @Test
    void helpNeedsNoPipeline() {
        EtlCli.Arguments args = EtlCli.Arguments.parse(new String[] {"--help"});

        assertTrue(args.help);
    }

    @Test
    void missingPipelineFails() {
        assertThrows(IllegalArgumentException.class,
                () -> EtlCli.Arguments.parse(new String[] {"--master", "local[*]"}));
    }

    @Test
    void unknownOptionFails() {
        assertThrows(IllegalArgumentException.class,
                () -> EtlCli.Arguments.parse(new String[] {"p.yaml", "--bogus"}));
    }

    @Test
    void malformedConfFails() {
        assertThrows(IllegalArgumentException.class,
                () -> EtlCli.Arguments.parse(new String[] {"p.yaml", "--conf", "novalue"}));
    }

    @Test
    void missingMasterValueFails() {
        assertThrows(IllegalArgumentException.class,
                () -> EtlCli.Arguments.parse(new String[] {"p.yaml", "--master"}));
    }
}
