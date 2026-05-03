package fr.insee.vtl.coverage;

import fr.insee.vtl.coverage.model.Folder;
import fr.insee.vtl.coverage.model.Test;
import fr.insee.vtl.coverage.tck.TckCaseExecutor;
import fr.insee.vtl.coverage.tck.TckFolders;
import fr.insee.vtl.coverage.tck.TckLeafCase;
import fr.insee.vtl.coverage.tck.TckSparkScriptEngines;
import java.io.InputStream;
import java.util.List;
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
   * {@link Named} is required so runners (e.g. IDEA) use the TCK path as the invocation title; a
   * bare {@code String} as first argument only yields {@code tckLeaf(String, Test)[n]}.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("leafCases")
  void tckLeaf(String displayPath, Test payload) throws Exception {
    executor.run(payload, displayPath);
  }

  static Stream<Arguments> leafCases() throws Exception {
    InputStream raw = TCKTest.class.getClassLoader().getResourceAsStream("v2.1.zip");
    Assumptions.assumeTrue(raw != null, "Skipping TCK tests: v2.1.zip not on classpath");

    try (InputStream in = raw) {
      List<Folder> roots = TCK.runTCK(in);
      List<TckLeafCase> leaves = TckFolders.collectLeaves(roots);
      return leaves.stream()
          .map(
              leaf -> {
                String path = leaf.displayPath();
                return Arguments.of(Named.of(path, path), leaf.payload());
              });
    }
  }
}
