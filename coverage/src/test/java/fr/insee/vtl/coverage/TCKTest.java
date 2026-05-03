package fr.insee.vtl.coverage;

import fr.insee.vtl.coverage.model.Folder;
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
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

/**
 * Spark-backed conformance run against the packaged TCK ({@code v2.1.zip}). Orchestration only —
 * discovery, naming, and assertions live under {@code fr.insee.vtl.coverage.tck}.
 */
class TCKTest {

  private TckCaseExecutor executor;

  @BeforeEach
  void setUp() {
    SparkSession spark = SparkSession.builder().appName("tck").master("local").getOrCreate();
    executor = new TckCaseExecutor(TckSparkScriptEngines.createVtlOnSpark(spark));
  }

  @TestFactory
  @DisplayName("TCK v2.1 (Spark)")
  Stream<DynamicTest> generateTests() {
    InputStream raw = getClass().getClassLoader().getResourceAsStream("v2.1.zip");
    Assumptions.assumeTrue(raw != null, "Skipping TCK tests: v2.1.zip not on classpath");

    try (InputStream in = raw) {
      List<Folder> roots = TCK.runTCK(in);
      List<TckLeafCase> leaves = TckFolders.collectLeaves(roots);
      return leaves.stream()
          .map(
              leaf ->
                  DynamicTest.dynamicTest(
                      leaf.displayPath(), () -> executor.run(leaf.payload(), leaf.displayPath())));
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new IllegalStateException("Failed to load or schedule TCK tests", e);
    }
  }
}
