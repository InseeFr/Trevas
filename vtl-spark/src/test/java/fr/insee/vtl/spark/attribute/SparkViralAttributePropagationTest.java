package fr.insee.vtl.spark.attribute;

import static fr.insee.vtl.model.Dataset.Role;
import static org.assertj.core.api.Assertions.assertThat;

import fr.insee.vtl.engine.VtlScriptEngine;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.InMemoryDataset;
import fr.insee.vtl.model.Structured;
import java.util.List;
import java.util.Map;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SparkViralAttributePropagationTest {

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
  void innerJoin_mergesHomonymViralAttributes() throws ScriptException {
    engine.put("left", viralDataset("Me_left", row("k1", 1L, "H"), row("k2", 2L, "P")));
    engine.put("right", viralDataset("Me_right", row("k1", 10L, "A"), row("k2", 20L, "Z")));
    engine.eval("res := inner_join(left, right using Id_1);");
    var res = (Dataset) engine.getContext().getAttribute("res");
    assertThat(res.getDataAsMap())
        .containsExactly(
            Map.of("Id_1", "k1", "Me_left", 1L, "Me_right", 10L, "At_1", "A"),
            Map.of("Id_1", "k2", "Me_left", 2L, "Me_right", 20L, "At_1", "P"));
  }

  @Test
  void groupedAggr_propagatesViralValuesPerGroup() throws ScriptException {
    engine.put("ds", multiIdentifierGroupedAggrDataset());
    engine.eval("res <- ds[aggr Me_2 := max(Me_1), Me_3 := min(Me_1) group by Id_1];");
    var res = (Dataset) engine.getContext().getAttribute("res");
    assertThat(res.getDataStructure().get("At_1").getRole()).isEqualTo(Role.ATTRIBUTE);
    assertGroupedAggrViralValues(res.getDataAsMap());
  }

  @Test
  void binaryDatasetFunction_mergesViralFromOperands() throws ScriptException {
    engine.put("ds1", viralDataset("Me_1", row("x", 10L, "M"), row("y", 20L, "P")));
    engine.put("ds2", viralDataset("Me_1", row("x", 1L, "N"), row("y", 30L, "Z")));
    engine.eval("res := ds1#Me_1 + ds2#Me_1;");
    var res = (Dataset) engine.getContext().getAttribute("res");
    assertThat(res.getDataStructure().containsKey("At_1")).isTrue();
    assertThat(res.getDataAsMap())
        .containsExactly(
            Map.of("Id_1", "x", "Me_1", 11L, "At_1", "M"),
            Map.of("Id_1", "y", "Me_1", 50L, "At_1", "P"));
  }

  private static InMemoryDataset viralDataset(String measureName, List<Object>... rows) {
    return new InMemoryDataset(
        List.of(rows),
        List.of(
            new Structured.Component("Id_1", String.class, Role.IDENTIFIER),
            new Structured.Component(measureName, Long.class, Role.MEASURE),
            new Structured.Component("At_1", String.class, Role.VIRALATTRIBUTE)));
  }

  private static List<Object> row(String id, long measure, String viral) {
    return List.of(id, measure, viral);
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
