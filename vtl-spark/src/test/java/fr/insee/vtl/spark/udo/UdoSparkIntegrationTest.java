package fr.insee.vtl.spark.udo;

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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Spark integration tests for user-defined operators. */
class UdoSparkIntegrationTest {

  private SparkSession spark;
  private ScriptEngine engine;

  @BeforeEach
  void setUp() {
    engine = new ScriptEngineManager().getEngineByExtension("vtl");
    spark = SparkSession.builder().appName("udo-test").master("local[1]").getOrCreate();
    SparkSession.setActiveSession(spark);
    engine.put(VtlScriptEngine.PROCESSING_ENGINE_NAMES, "spark");
  }

  @AfterEach
  void tearDown() {
    if (spark != null) {
      spark.close();
    }
  }

  @Test
  void scalarUdoOnSpark() throws ScriptException {
    engine.eval(
        """
        define operator add (x integer default 0, y integer default 0)
           returns number is
              x + y
        end operator;
        res := add(10, 32);
        """);
    assertThat(engine.getContext().getAttribute("res")).isEqualTo(42L);
  }

  @Test
  void datasetFilterUdoOnSpark() throws ScriptException {
    engine.put("ds1", filterSampleDataset());
    engine.eval(
        """
        define operator keep_age_gt (ds dataset, threshold integer)
           returns dataset is
              ds[filter age > threshold]
        end operator;
        res := keep_age_gt(ds1, 10);
        """);
    Dataset res = (Dataset) engine.getContext().getAttribute("res");
    assertThat(res.getDataAsMap())
        .extracting(row -> row.get("name"))
        .containsExactlyInAnyOrder("Nico", "Franck");
  }

  @Test
  void calcDatasetUdoOnSpark() throws ScriptException {
    engine.put("ds2", filterSampleDataset());
    engine.eval(
        """
        define operator with_double_age (ds dataset)
           returns dataset is
              ds[calc age_x2 := age * 2]
        end operator;
        res := with_double_age(ds2);
        """);
    Dataset res = (Dataset) engine.getContext().getAttribute("res");
    assertThat(res.getDataAsMap()).anySatisfy(row -> row.containsKey("age_x2"));
  }

  private static InMemoryDataset filterSampleDataset() {
    return new InMemoryDataset(
        List.of(
            Map.of("name", "Hadrien", "age", 10L),
            Map.of("name", "Nico", "age", 11L),
            Map.of("name", "Franck", "age", 12L)),
        Map.of("name", String.class, "age", Long.class),
        Map.of("name", Role.IDENTIFIER, "age", Role.MEASURE));
  }
}
