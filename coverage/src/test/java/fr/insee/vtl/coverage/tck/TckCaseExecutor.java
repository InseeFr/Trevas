package fr.insee.vtl.coverage.tck;

import fr.insee.vtl.coverage.model.Test;
import fr.insee.vtl.model.Dataset;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.SimpleBindings;
import org.assertj.core.api.SoftAssertions;

/**
 * Runs one TCK leaf: bind inputs, {@code eval} script, compare engine bindings to expected outputs.
 */
public final class TckCaseExecutor {

  private final ScriptEngine engine;

  public TckCaseExecutor(ScriptEngine engine) {
    this.engine = Objects.requireNonNull(engine, "engine");
  }

  public void run(Test test, String displayPath) throws Exception {
    Objects.requireNonNull(test, "test");
    Objects.requireNonNull(displayPath, "displayPath");

    if (!test.getInputFixtureIssues().isEmpty()) {
      throw new AssertionError(
          TckFailureText.tckFixtureInconsistency(
              displayPath, test, "input", test.getInputFixtureIssues()));
    }
    if (!test.getOutputFixtureIssues().isEmpty()) {
      throw new AssertionError(
          TckFailureText.tckFixtureInconsistency(
              displayPath, test, "expected output", test.getOutputFixtureIssues()));
    }

    Bindings bindings = new SimpleBindings();
    Map<String, Dataset> inputs = test.getInput();
    if (inputs != null) {
      bindings.putAll(inputs);
    }

    engine.getContext().setBindings(bindings, ScriptContext.ENGINE_SCOPE);

    SoftAssertions softly = new SoftAssertions();
    String script = test.getScript();
    if (script == null) {
      softly.fail("[" + displayPath + "] missing transformation script");
      softly.assertAll();
      return;
    }
    try {
      engine.eval(script);
    } catch (Throwable t) {
      throw fixtureOrExecutionError(displayPath, test, t);
    }

    Map<String, Dataset> outputs = test.getOutputs();
    if (outputs == null || outputs.isEmpty()) {
      softly.fail("[" + displayPath + "] TCK case has no expected outputs");
      softly.assertAll();
      return;
    }

    outputs.entrySet().stream()
        .sorted(Map.Entry.comparingByKey())
        .forEach(
            entry -> {
              String outputName = entry.getKey();
              Dataset expected = entry.getValue();
              Object actualBinding = engine.getContext().getAttribute(outputName);
              softly
                  .assertThat(actualBinding)
                  .as("[%s] output `%s` must be a Dataset", displayPath, outputName)
                  .isInstanceOf(Dataset.class);
              if (!(actualBinding instanceof Dataset)) {
                return;
              }
              Dataset actual = (Dataset) actualBinding;
              if (!actual.getDataStructure().equals(expected.getDataStructure())) {
                softly.fail(
                    TckFailureText.structureMismatch(
                        displayPath,
                        outputName,
                        actual.getDataStructure(),
                        expected.getDataStructure()));
                return;
              }
              var actualMap =
                  readDataMapOrThrowFixtureError(
                      displayPath, test, outputName, actual, "Trevas result", "Trevas result");
              var expectedMap =
                  readDataMapOrThrowFixtureError(
                      displayPath,
                      test,
                      outputName,
                      expected,
                      "expected output",
                      "TCK expected output");
              if (!TckDatasetComparison.sameRows(actualMap, expectedMap)) {
                softly.fail(
                    TckFailureText.rowDataMismatch(
                        displayPath, outputName, test, actual, expected));
              }
            });
    softly.assertAll();
  }

  private static AssertionError fixtureOrExecutionError(
      String displayPath, Test test, Throwable error) {
    if (TckInputValidator.isTckFixtureLoadFailure(error)) {
      return new AssertionError(
          TckFailureText.tckFixtureInconsistencyFromRuntime(
              displayPath, test, "input", "input dataset", error),
          error);
    }
    return new AssertionError(TckFailureText.executionError(displayPath, test, error), error);
  }

  private static List<Map<String, Object>> readDataMapOrThrowFixtureError(
      String displayPath,
      Test test,
      String datasetName,
      Dataset dataset,
      String fixtureRole,
      String roleLabel) {
    try {
      return dataset.getDataAsMap();
    } catch (RuntimeException error) {
      if (TckInputValidator.isTckFixtureLoadFailure(error)) {
        throw new AssertionError(
            TckFailureText.tckFixtureInconsistencyFromRuntime(
                displayPath, test, fixtureRole, datasetName, error),
            error);
      }
      throw new AssertionError(
          "[" + displayPath + "] could not read " + roleLabel + " `" + datasetName + "`", error);
    }
  }
}
