package fr.insee.vtl.spark.processing.engine;

import static org.assertj.core.api.Assertions.assertThat;

import fr.insee.vtl.engine.VtlScriptEngine;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.InMemoryDataset;
import fr.insee.vtl.model.Structured;
import java.util.Arrays;
import java.util.List;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class PivotTest {

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
  public void testLeftJoin() throws ScriptException {

    var dataset =
        new InMemoryDataset(
            List.of(
                List.of(1L, "A", 5L, "E"),
                List.of(1L, "B", 2L, "F"),
                List.of(1L, "C", 7L, "F"),
                List.of(2L, "A", 3L, "E"),
                List.of(2L, "B", 4L, "E"),
                List.of(2L, "C", 9L, "F")),
            List.of(
                new Structured.Component("Id_1", Long.class, Dataset.Role.IDENTIFIER),
                new Structured.Component("Id_2", String.class, Dataset.Role.IDENTIFIER),
                new Structured.Component("Me_1", Long.class, Dataset.Role.MEASURE),
                new Structured.Component("At_1", String.class, Dataset.Role.ATTRIBUTE)));

    ScriptContext context = engine.getContext();
    context.getBindings(ScriptContext.ENGINE_SCOPE).put("Ds_1", dataset);

    engine.eval("DS_r := Ds_1 [ pivot Id_2, Me_1 ]");

    var result = (Dataset) context.getAttribute("DS_r");
    assertThat(result.getDataAsList())
        .containsExactlyInAnyOrder(Arrays.asList(1L, 5L, 2L, 7L), Arrays.asList(2L, 3L, 4L, 9L));
  }
}
