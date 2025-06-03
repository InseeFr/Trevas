package fr.insee.vtl.spark.processing.engine;

import static org.assertj.core.api.Assertions.assertThat;

import fr.insee.vtl.engine.VtlScriptEngine;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.InMemoryDataset;
import fr.insee.vtl.model.Structured;
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

public class FilterTest {

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

  @Test
  public void testFilterClause() throws ScriptException {

    ScriptContext context = engine.getContext();
    context.setAttribute("ds1", dataset, ScriptContext.ENGINE_SCOPE);

    engine.eval("ds := ds1[filter age > 10 and age < 12];");

    assertThat(engine.getContext().getAttribute("ds")).isInstanceOf(Dataset.class);
    var ds = (Dataset) engine.getContext().getAttribute("ds");
    assertThat(ds.getDataAsMap())
        .isEqualTo(List.of(Map.of("name", "Nico", "age", 11L, "weight", 10L)));

    assertThat(ds.getDataStructure())
        .containsValues(
            new Structured.Component("name", String.class, Dataset.Role.IDENTIFIER),
            new Structured.Component("age", Long.class, Dataset.Role.MEASURE),
            new Structured.Component("weight", Long.class, Dataset.Role.MEASURE));
  }
}
