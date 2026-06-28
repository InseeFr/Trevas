package fr.insee.vtl.spark.attribute;

import static fr.insee.vtl.model.Dataset.Role;
import static org.assertj.core.api.Assertions.assertThat;

import fr.insee.vtl.engine.VtlScriptEngine;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.InMemoryDataset;
import java.util.List;
import java.util.Map;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Spark-specific viral attribute integration tests. */
class ViralAttributeSparkIntegrationTest {

  private static SparkSession spark;
  private ScriptEngine engine;

  @BeforeEach
  void setUp() {
    spark =
        SparkSession.builder()
            .appName("viral-test")
            .master("local[1]")
            .config("spark.ui.enabled", "false")
            .getOrCreate();
    engine = new ScriptEngineManager().getEngineByExtension("vtl");
    engine.put(VtlScriptEngine.PROCESSING_ENGINE_NAMES, "spark");
    engine.put("$vtl.spark.session", spark);
  }

  @AfterAll
  static void tearDown() {
    if (spark != null) {
      spark.stop();
    }
  }

  @Test
  void groupedAggr_propagatesViralValuesPerGroup() throws ScriptException {
    engine.put("ds", multiIdentifierGroupedAggrDataset());
    engine.eval("res <- ds[aggr Me_2 := max(Me_1), Me_3 := min(Me_1) group by Id_1];");
    var res = (Dataset) engine.getContext().getAttribute("res");
    assertThat(res.getDataStructure().get("At_1").getRole()).isEqualTo(Role.VIRALATTRIBUTE);
    assertGroupedAggrViralValues(res.getDataAsMap());
  }

  private static InMemoryDataset multiIdentifierGroupedAggrDataset() {
    return new InMemoryDataset(
        List.of(
            aggrRow(2010L, "E", "XX", 20L, ""),
            aggrRow(2010L, "B", "XX", 1L, "H"),
            aggrRow(2010L, "R", "XX", 1L, "A"),
            aggrRow(2010L, "F", "YY", 23L, ""),
            aggrRow(2011L, "E", "XX", 20L, "P"),
            aggrRow(2011L, "B", "ZZ", 1L, "N"),
            aggrRow(2011L, "R", "YY", -1L, "P"),
            aggrRow(2011L, "F", "XX", 20L, "Z"),
            aggrRow(2012L, "L", "ZZ", 40L, "P"),
            aggrRow(2012L, "E", "YY", 30L, "P")),
        Map.of(
            "Id_1", Long.class,
            "Id_2", String.class,
            "Id_3", String.class,
            "Me_1", Long.class,
            "At_1", String.class),
        Map.of(
            "Id_1", Role.IDENTIFIER,
            "Id_2", Role.IDENTIFIER,
            "Id_3", Role.IDENTIFIER,
            "Me_1", Role.MEASURE,
            "At_1", Role.VIRALATTRIBUTE));
  }

  private static void assertGroupedAggrViralValues(List<Map<String, Object>> rows) {
    assertThat(rows).hasSize(3);
    assertThat(findRowById1(rows, 2010L))
        .containsEntry("Me_2", 23L)
        .containsEntry("Me_3", 1L)
        .containsEntry("At_1", "");
    assertThat(findRowById1(rows, 2011L))
        .containsEntry("Me_2", 20L)
        .containsEntry("Me_3", -1L)
        .containsEntry("At_1", "N");
    assertThat(findRowById1(rows, 2012L))
        .containsEntry("Me_2", 40L)
        .containsEntry("Me_3", 30L)
        .containsEntry("At_1", "P");
  }

  private static Map<String, Object> aggrRow(
      long id1, String id2, String id3, long me1, String at1) {
    return Map.of("Id_1", id1, "Id_2", id2, "Id_3", id3, "Me_1", me1, "At_1", at1);
  }

  private static Map<String, Object> findRowById1(List<Map<String, Object>> rows, long id1) {
    return rows.stream()
        .filter(row -> id1 == ((Number) row.get("Id_1")).longValue())
        .findFirst()
        .orElseThrow();
  }
}
