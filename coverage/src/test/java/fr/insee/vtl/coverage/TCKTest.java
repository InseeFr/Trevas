package fr.insee.vtl.coverage;

import fr.insee.vtl.coverage.model.Folder;
import fr.insee.vtl.coverage.model.Test;
import fr.insee.vtl.engine.VtlScriptEngine;
import fr.insee.vtl.model.Dataset;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import javax.script.*;
import org.apache.spark.sql.SparkSession;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.*;

class TCKTest {

  /** Separator for hierarchical names (JUnit / Surefire friendly, readable in GitHub reports). */
  private static final String PATH_SEP = " \u00BB ";

  private ScriptEngine engine;

  @BeforeEach
  public void setUp() {
    SparkSession spark = SparkSession.builder().appName("test").master("local").getOrCreate();

    ScriptEngineManager mgr = new ScriptEngineManager();
    engine = mgr.getEngineByExtension("vtl");
    engine.put(VtlScriptEngine.PROCESSING_ENGINE_NAMES, "spark");
    engine.put("$vtl.spark.session", spark);
  }

  @TestFactory
  Stream<DynamicNode> generateTests() {
    InputStream in = getClass().getClassLoader().getResourceAsStream("v2.1.zip");
    // Skip the test factory entirely if file is not present
    Assumptions.assumeTrue(in != null, "Skipping TCK tests: resource file not found");

    List<Folder> tests = TCK.runTCK(in);
    return tests.stream().map(f -> toDynamicNode(f, ""));
  }

  private static String displayPath(String prefix, String segment) {
    return prefix.isEmpty() ? segment : prefix + PATH_SEP + segment;
  }

  private DynamicNode toDynamicNode(Folder folder, String prefix) {
    String name = folder.getName();
    String path = displayPath(prefix, name);

    if (folder.getTest() != null) {
      return DynamicTest.dynamicTest(path, () -> runTckCase(folder.getTest(), path));
    }

    List<DynamicNode> children = new ArrayList<>();
    if (folder.getFolders() != null) {
      for (Folder sub : folder.getFolders()) {
        children.add(toDynamicNode(sub, path));
      }
    }
    return DynamicContainer.dynamicContainer(path, children.stream());
  }

  private void runTckCase(Test test, String displayPath) throws Exception {
    String script = test.getScript();
    Map<String, Dataset> inputs = test.getInput();

    Bindings bindings = new SimpleBindings();
    bindings.putAll(inputs);

    engine.getContext().setBindings(bindings, ScriptContext.ENGINE_SCOPE);
    engine.eval(script);

    Map<String, Dataset> outputs = test.getOutputs();
    SoftAssertions softly = new SoftAssertions();
    outputs.forEach(
        (outputName, tckDataset) -> {
          Object trevasValue = engine.getContext().getAttribute(outputName);
          softly
              .assertThat(trevasValue)
              .as("[%s] output `%s` must be a Dataset", displayPath, outputName)
              .isInstanceOf(Dataset.class);
          if (!(trevasValue instanceof Dataset)) {
            return;
          }
          Dataset trevasDataset = (Dataset) trevasValue;
          softly
              .assertThat(trevasDataset.getDataStructure())
              .as("[%s] output `%s` data structure", displayPath, outputName)
              .isEqualTo(tckDataset.getDataStructure());
          softly
              .assertThat(trevasDataset.getDataAsMap())
              .as("[%s] output `%s` row data", displayPath, outputName)
              .containsExactlyElementsOf(tckDataset.getDataAsMap());
        });
    softly.assertAll();
  }
}
