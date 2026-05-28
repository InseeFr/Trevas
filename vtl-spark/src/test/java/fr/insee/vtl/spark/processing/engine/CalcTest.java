package fr.insee.vtl.spark.processing.engine;

import static org.assertj.core.api.Assertions.assertThat;

import fr.insee.vtl.engine.VtlScriptEngine;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.InMemoryDataset;
import fr.insee.vtl.model.Structured;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class CalcTest {

  private SparkSession spark;
  private ScriptEngine engine;

  @BeforeEach
  public void setUp() {

    ScriptEngineManager mgr = new ScriptEngineManager();
    engine = mgr.getEngineByExtension("vtl");

    spark = SparkSession.builder().appName("test").master("local").getOrCreate();
    SparkSession.setActiveSession(spark);

    engine.put(VtlScriptEngine.PROCESSING_ENGINE_NAMES, "spark");
  }

  @AfterEach
  public void tearDown() {
    if (spark != null) spark.close();
  }

  InMemoryDataset dataset =
      new InMemoryDataset(
          List.of(
              Map.of("name", "Hadrien", "age", 10L, "weight", 11L),
              Map.of("name", "Nico", "age", 11L, "weight", 10L),
              Map.of("name", "Franck", "age", 12L, "weight", 9L)),
          Map.of("name", String.class, "age", Long.class, "weight", Long.class),
          Map.of(
              "name",
              Dataset.Role.IDENTIFIER,
              "age",
              Dataset.Role.MEASURE,
              "weight",
              Dataset.Role.MEASURE));

  @Test
  public void testCalcClause() throws ScriptException, InterruptedException {
    ScriptContext context = engine.getContext();
    context.setAttribute("ds1", dataset, ScriptContext.ENGINE_SCOPE);

    engine.eval(
        "ds := ds1[calc test := between(age, 10, 11), age := age * 2, attribute wisdom := (weight + age) / 2];");

    var ds = (Dataset) engine.getContext().getAttribute("ds");
    assertThat(ds).isInstanceOf(Dataset.class);
    assertThat(ds.getDataAsMap())
        .isEqualTo(
            List.of(
                Map.of("name", "Hadrien", "age", 20L, "test", true, "weight", 11L, "wisdom", 10.5D),
                Map.of("name", "Nico", "age", 22L, "test", true, "weight", 10L, "wisdom", 10.5D),
                Map.of(
                    "name", "Franck", "age", 24L, "test", false, "weight", 9L, "wisdom", 10.5D)));
    assertThat(ds.getDataStructure())
        .containsValues(
            new Structured.Component("name", String.class, Dataset.Role.IDENTIFIER),
            new Structured.Component("age", Long.class, Dataset.Role.MEASURE),
            new Structured.Component("test", Boolean.class, Dataset.Role.MEASURE),
            new Structured.Component("weight", Long.class, Dataset.Role.MEASURE),
            new Structured.Component("wisdom", Double.class, Dataset.Role.ATTRIBUTE));
  }

  @Test
  public void testCaseElseAppliesWhenWhenConditionIsNull() throws ScriptException {
    ScriptContext context = engine.getContext();

    Map<String, Object> r4 = new HashMap<>();
    r4.put("name", "NullGuy");
    r4.put("me_1", null);

    InMemoryDataset dsWithNull =
        new InMemoryDataset(
            List.of(
                Map.of("name", "A", "me_1", 0.12D),
                Map.of("name", "B", "me_1", 3.5D),
                Map.of("name", "C", "me_1", 10.7D),
                r4),
            Map.of("name", String.class, "me_1", Double.class),
            Map.of("name", Dataset.Role.IDENTIFIER, "me_1", Dataset.Role.MEASURE));

    context.setAttribute("ds_null", dsWithNull, ScriptContext.ENGINE_SCOPE);
    engine.eval(
        "res := ds_null[calc me_2 := case when me_1 <= 1 then 0 when me_1 > 1 and me_1 <= 10 then 1 when me_1 > 10 then 10 else 100];");

    Dataset res = (Dataset) context.getAttribute("res");
    assertThat(res.getDataAsMap())
        .contains(
            Map.of("name", "A", "me_1", 0.12D, "me_2", 0L),
            Map.of("name", "B", "me_1", 3.5D, "me_2", 1L),
            Map.of("name", "C", "me_1", 10.7D, "me_2", 10L));

    Map<String, Object> expectedNullRow = new HashMap<>();
    expectedNullRow.put("name", "NullGuy");
    expectedNullRow.put("me_1", null);
    expectedNullRow.put("me_2", 100L);
    assertThat(res.getDataAsMap()).contains(expectedNullRow);
  }
}
