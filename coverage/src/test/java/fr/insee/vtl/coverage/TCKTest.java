package fr.insee.vtl.coverage;

import fr.insee.vtl.coverage.model.Folder;
import fr.insee.vtl.coverage.model.Test;
import fr.insee.vtl.coverage.tck.TckCaseExecutor;
import fr.insee.vtl.coverage.tck.TckFolders;
import fr.insee.vtl.coverage.tck.TckLeafCase;
import fr.insee.vtl.coverage.tck.TckSparkScriptEngines;
import java.io.InputStream;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Spark-backed conformance run against the packaged TCK ({@code v2.1.zip}). Orchestration only —
 * discovery, naming, and assertions live under {@code fr.insee.vtl.coverage.tck}.
 */
@DisplayName("TCK v2.1 (Spark)")
class TCKTest {

  private TckCaseExecutor executor;

  @BeforeEach
  void setUp() {
    SparkSession spark = SparkSession.builder().appName("tck").master("local").getOrCreate();
    executor = new TckCaseExecutor(TckSparkScriptEngines.createVtlOnSpark(spark));
  }

  /**
   * Surefire XML currently persists the technical method signature for parameterized invocations.
   * Emit explicit, stable labels in stdout and failure wrappers so GitHub logs stay readable.
   */
  @ParameterizedTest(name = "tckLeaf[{0}]")
  @MethodSource("leafCases")
  void tckLeaf(int testIndex, String displayPath, Test payload) throws Exception {
    String label = "Test " + testIndex + " — " + displayPath;
    try {
      executor.run(payload, displayPath);
      System.out.println("✅ " + label);
    } catch (Throwable t) {
      String details = t.getMessage() == null ? "" : System.lineSeparator() + t.getMessage();
      throw new AssertionError("❌ " + label + details, t);
    }
  }

  static Stream<Arguments> leafCases() throws Exception {
    InputStream raw = TCKTest.class.getClassLoader().getResourceAsStream("v2.1.zip");
    Assumptions.assumeTrue(raw != null, "Skipping TCK tests: v2.1.zip not on classpath");

    try (InputStream in = raw) {
      List<Folder> roots = TCK.runTCK(in);
      List<TckLeafCase> leaves = TckFolders.collectLeaves(roots);
      return IntStream.range(0, leaves.size())
          .mapToObj(
              i -> {
                TckLeafCase leaf = leaves.get(i);
                String path = leaf.displayPath();
                return Arguments.of(i + 1, Named.of(path, path), leaf.payload());
              });
    }
  }
}
