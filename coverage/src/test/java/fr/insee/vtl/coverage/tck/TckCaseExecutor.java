package fr.insee.vtl.coverage.tck;

import fr.insee.vtl.coverage.model.Test;
import fr.insee.vtl.model.Dataset;
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
      throw new AssertionError(TckFailureText.executionError(displayPath, test, t), t);
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
              if (!TckDatasetComparison.sameRowOrder(
                  actual.getDataAsMap(), expected.getDataAsMap())) {
                softly.fail(
                    TckFailureText.rowDataMismatch(
                        displayPath, outputName, test, actual, expected));
              }
            });
    softly.assertAll();
  }
}
