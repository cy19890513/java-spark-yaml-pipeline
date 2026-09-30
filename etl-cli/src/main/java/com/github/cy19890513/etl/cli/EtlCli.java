package com.github.cy19890513.etl.cli;

import com.github.cy19890513.etl.api.StageRegistry;
import com.github.cy19890513.etl.api.definition.PipelineDefinition;
import com.github.cy19890513.etl.connectors.BuiltinStages;
import com.github.cy19890513.etl.core.PipelineLoader;
import com.github.cy19890513.etl.core.PipelineRunner;
import com.github.cy19890513.etl.core.PipelineValidationException;
import com.github.cy19890513.etl.core.QualityCheckFailedException;
import com.github.cy19890513.etl.quality.QualityChecks;
import java.io.IOException;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Command-line entry point: runs a pipeline YAML file on Spark.
 *
 * <p>Usage:
 * <pre>
 * java -jar etl-cli-&lt;version&gt;.jar &lt;pipeline.yaml&gt; [--master &lt;url&gt;] [--conf &lt;key=value&gt;]...
 * </pre>
 *
 * <p>Exit codes: 0 on success, 1 on bad arguments or unreadable YAML,
 * 2 on validation failure, 3 on a fail-fast quality abort, 4 on any other
 * error.
 */
public final class EtlCli {

    private static final Logger log = LoggerFactory.getLogger(EtlCli.class);

    private EtlCli() {
        // entry point class
    }

    /**
     * CLI entry point.
     *
     * @param args command-line arguments
     */
    public static void main(String[] args) {
        System.exit(run(args));
    }

    /**
     * Runs the CLI, returning the process exit code (kept separate from
     * {@link #main(String[])} for testability).
     *
     * @param args command-line arguments
     * @return exit code
     */
    static int run(String[] args) {
        Arguments parsed;
        try {
            parsed = Arguments.parse(args);
        } catch (IllegalArgumentException e) {
            System.err.println("Error: " + e.getMessage());
            printUsage();
            return 1;
        }
        if (parsed.help) {
            printUsage();
            return 0;
        }

        PipelineDefinition definition;
        try {
            definition = new PipelineLoader().load(parsed.pipeline);
        } catch (IOException e) {
            System.err.println("Error: cannot load pipeline YAML: " + e.getMessage());
            return 1;
        }

        StageRegistry registry = new StageRegistry();
        BuiltinStages.registerAll(registry);
        QualityChecks.registerAll(registry);

        Map<String, String> sparkConf = new LinkedHashMap<>(parsed.sparkConf);
        sparkConf.putIfAbsent("spark.master", "local[*]");
        log.info("Starting pipeline '{}' on master '{}'",
                definition.getName(), sparkConf.get("spark.master"));

        try {
            new PipelineRunner(registry, sparkConf).run(definition);
        } catch (PipelineValidationException e) {
            System.err.println(e.getMessage());
            return 2;
        } catch (QualityCheckFailedException e) {
            System.err.println("Aborted: " + e.getMessage());
            return 3;
        } catch (Exception e) {
            System.err.println("Error: pipeline failed: " + e.getMessage());
            return 4;
        }
        System.out.println("Pipeline '" + definition.getName() + "' completed successfully.");
        return 0;
    }

    private static void printUsage() {
        System.out.println("Usage: java -jar etl-cli-<version>.jar <pipeline.yaml> [options]");
        System.out.println("Options:");
        System.out.println("  --master <url>       Spark master URL (default: local[*])");
        System.out.println("  --conf <key=value>   Extra Spark configuration (repeatable)");
        System.out.println("  --help               Show this help");
    }

    /** Parsed command-line arguments. */
    static final class Arguments {
        Path pipeline;
        boolean help;
        final Map<String, String> sparkConf = new LinkedHashMap<>();

        static Arguments parse(String[] args) {
            Arguments parsed = new Arguments();
            for (int i = 0; i < args.length; i++) {
                switch (args[i]) {
                    case "--help" -> parsed.help = true;
                    case "--master" -> {
                        String master = next(args, i, "--master");
                        parsed.sparkConf.put("spark.master", master);
                        i++;
                    }
                    case "--conf" -> {
                        String pair = next(args, i, "--conf");
                        int eq = pair.indexOf('=');
                        if (eq <= 0) {
                            throw new IllegalArgumentException(
                                    "--conf expects key=value, got '" + pair + "'");
                        }
                        parsed.sparkConf.put(pair.substring(0, eq), pair.substring(eq + 1));
                        i++;
                    }
                    default -> {
                        if (args[i].startsWith("--")) {
                            throw new IllegalArgumentException("Unknown option '" + args[i] + "'");
                        }
                        if (parsed.pipeline != null) {
                            throw new IllegalArgumentException(
                                    "Only one pipeline YAML file is accepted");
                        }
                        parsed.pipeline = Path.of(args[i]);
                    }
                }
            }
            if (!parsed.help && parsed.pipeline == null) {
                throw new IllegalArgumentException("A pipeline YAML file is required");
            }
            return parsed;
        }

        private static String next(String[] args, int i, String option) {
            if (i + 1 >= args.length) {
                throw new IllegalArgumentException("Option '" + option + "' needs a value");
            }
            return args[i + 1];
        }
    }
}
