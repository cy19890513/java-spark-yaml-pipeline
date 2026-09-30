package com.github.cy19890513.etl.core;

import com.github.cy19890513.etl.api.definition.PipelineDefinition;
import com.github.cy19890513.etl.api.definition.QualityDefinition;
import com.github.cy19890513.etl.api.definition.SinkDefinition;
import com.github.cy19890513.etl.api.definition.SourceDefinition;
import com.github.cy19890513.etl.api.definition.StageDefinition;
import com.github.cy19890513.etl.api.definition.TransformDefinition;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Structural validator for {@link PipelineDefinition}.
 *
 * <p>Checks, without touching Spark:
 * <ul>
 *   <li>the pipeline has a name and at least one source;</li>
 *   <li>every stage has a name, a type, and a name unique across the pipeline;</li>
 *   <li>every transform/quality/sink input references an existing stage that
 *       produces a DataFrame (a source or a transform);</li>
 *   <li>no stage lists itself as an input.</li>
 * </ul>
 *
 * <p>Cycle detection and type resolution happen later: cycles in the DAG
 * builder, unknown types in the {@code StageRegistry} at run time.
 */
public final class PipelineValidator {

    private PipelineValidator() {
        // utility class
    }

    /**
     * Validates the pipeline definition.
     *
     * @param definition the definition to validate
     * @throws PipelineValidationException listing every problem found
     */
    public static void validate(PipelineDefinition definition) {
        List<String> errors = new ArrayList<>();

        if (definition == null) {
            throw new PipelineValidationException(List.of("Pipeline definition is null"));
        }
        if (isBlank(definition.getName())) {
            errors.add("Pipeline 'name' is required");
        }
        if (definition.getSources().isEmpty()) {
            errors.add("Pipeline must declare at least one source");
        }

        Set<String> names = new HashSet<>();
        Set<String> producers = new HashSet<>();
        List<StageDefinition> all = new ArrayList<>();
        all.addAll(definition.getSources());
        all.addAll(definition.getTransforms());
        all.addAll(definition.getQuality());
        all.addAll(definition.getSinks());

        for (StageDefinition stage : all) {
            String label = describe(stage);
            if (isBlank(stage.getName())) {
                errors.add(label + ": 'name' is required");
            } else if (!names.add(stage.getName())) {
                errors.add("Duplicate stage name: '" + stage.getName() + "'");
            }
            if (isBlank(stage.getType())) {
                errors.add(label + ": 'type' is required");
            }
        }
        for (SourceDefinition source : definition.getSources()) {
            if (!isBlank(source.getName())) {
                producers.add(source.getName());
            }
        }
        for (TransformDefinition transform : definition.getTransforms()) {
            if (!isBlank(transform.getName())) {
                producers.add(transform.getName());
            }
            checkInputs(transform.getName(), transform.getInputs(), names, producers, errors);
        }
        for (QualityDefinition check : definition.getQuality()) {
            checkInputs(check.getName(), singleInput(check.getInput()), names, producers, errors);
        }
        for (SinkDefinition sink : definition.getSinks()) {
            checkInputs(sink.getName(), singleInput(sink.getInput()), names, producers, errors);
        }

        if (!errors.isEmpty()) {
            throw new PipelineValidationException(errors);
        }
    }

    private static void checkInputs(String stageName, List<String> inputs,
            Set<String> names, Set<String> producers, List<String> errors) {
        if (inputs.isEmpty()) {
            errors.add("Stage '" + stageName + "': at least one input is required");
            return;
        }
        for (String input : inputs) {
            if (isBlank(input)) {
                errors.add("Stage '" + stageName + "': input name must not be blank");
            } else if (input.equals(stageName)) {
                errors.add("Stage '" + stageName + "': a stage cannot consume its own output");
            } else if (!names.contains(input)) {
                errors.add("Stage '" + stageName + "': unknown input '" + input + "'");
            } else if (!producers.contains(input)) {
                errors.add("Stage '" + stageName + "': input '" + input
                        + "' does not produce a DataFrame");
            }
        }
    }

    private static List<String> singleInput(String input) {
        return input == null ? List.of() : List.of(input);
    }

    private static String describe(StageDefinition stage) {
        return "Stage '" + (isBlank(stage.getName()) ? "?" : stage.getName()) + "'";
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
