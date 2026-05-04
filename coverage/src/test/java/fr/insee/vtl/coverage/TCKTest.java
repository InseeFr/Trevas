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
   * One argument + {@link Named} so {@code name = "{0}"} resolves to a stable label in Surefire XML
   * (see {@code coverage/pom.xml} phrased reporters). Stdout uses a two-line layout for CI logs.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("leafCases")
  void tckCase(TckCase c) throws Exception {
    try {
      executor.run(c.payload(), c.displayPath());
      logCaseOutcome(c, true);
    } catch (Throwable t) {
      logCaseOutcome(c, false);
      throw t;
    }
  }

  private static void logCaseOutcome(TckCase c, boolean success) {
    System.out.println((success ? "✅" : "❌") + " Test " + c.index());
    System.out.println("\t" + c.displayPath());
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
                TckCase c = new TckCase(i + 1, leaf.displayPath(), leaf.payload());
                return Arguments.of(Named.of(c.label(), c));
              });
    }
  }

  private record TckCase(int index, String displayPath, Test payload) {
    String label() {
      return "Test " + index + " — " + displayPath;
    }
  }
}
