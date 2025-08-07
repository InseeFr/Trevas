package fr.insee.vtl.spark.processing.engine;

import static org.assertj.core.api.Assertions.assertThat;

import fr.insee.vtl.engine.VtlScriptEngine;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.spark.samples.DatasetSamples;
import java.util.Map;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class OperatorsTest {

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
  public void testOperators() throws ScriptException {

    ScriptContext context = engine.getContext();
    context.setAttribute("ds1", DatasetSamples.ds1, ScriptContext.ENGINE_SCOPE);
    context.setAttribute("ds2", DatasetSamples.ds2, ScriptContext.ENGINE_SCOPE);

    engine.eval(
        "res := ds1#long1; "
            + "res1 := isnull(ds1); "
            + "ds_1 := ds1[keep id, long1, double1]; ds_2 := ds2[keep id, long1, double1]; "
            + "res2 := ds_1 + ds_2; "
            + "res3 := ds_1 - ds_2; "
            + "res4 := ds_1 * ds_2; "
            + "res5 := ds_1 / ds_2; "
            + "res6 := ds_1 = ds_2; "
            + "res7 := ds_1 <> ds_2; "
            + "res8 := ds_1 < ds_2; "
            + "res9 := ds_1 <= ds_2; "
            + "res10 := ds_1 > ds_2; "
            + "res11 := ds_1 >= ds_2; "
            + "res12 := + ds_1; "
            + "res13 := - ds_1; "
            + "res14 := ceil(floor(ln(exp(abs(ds_1))))); "
            + "res15 := round(ds_1, 5); "
            + "res16 := trunc(ds_1, 5); "
            + "res17 := sqrt(abs(ds_1)); "
            + "res18 := mod(ds_1, 5); "
            + "res19 := power(ds_1, 5); "
            + "res20 := log(abs(ds_1), 5); "
            + "ds_11 := ds1[keep id, string1, string2]; ds_22 := ds2[keep id, string1][calc string2 := string1]; "
            + "res21 := ds_11 || ds_22; ");
    var res = engine.getContext().getAttribute("res21");
    assertThat(((Dataset) res).getDataStructure().get("string1").getType()).isEqualTo(String.class);
  }

  @Test
  public void testPlan() throws ScriptException {
    engine.getContext().setAttribute("ds_1", DatasetSamples.ds1, ScriptContext.ENGINE_SCOPE);
    engine.getContext().setAttribute("ds_2", DatasetSamples.ds2, ScriptContext.ENGINE_SCOPE);
    engine.eval(
        "ds1 := ds_1[keep id, long1][rename long1 to bool_var]; "
            + "ds2 := ds_2[keep id, long1][rename long1 to bool_var]; "
            + "res := if ds1 > ds2 then ds1 else ds2;");
    var res = engine.getContext().getAttribute("res");
    assertThat(((Dataset) res).getDataAsMap())
        .containsExactlyInAnyOrder(
            Map.of("id", "Hadrien", "bool_var", 150L),
            Map.of("id", "Nico", "bool_var", 20L),
            Map.of("id", "Franck", "bool_var", 100L));
    assertThat(((Dataset) res).getDataStructure().get("bool_var").getType()).isEqualTo(Long.class);
  }
}
