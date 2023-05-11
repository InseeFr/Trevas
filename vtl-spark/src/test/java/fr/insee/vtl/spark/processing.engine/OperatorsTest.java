package fr.insee.vtl.spark.processing.engine;

import fr.insee.vtl.engine.VtlScriptEngine;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.spark.samples.DatasetSamples;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class OperatorsTest {

    private SparkSession spark;
    private ScriptEngine engine;

    @BeforeEach
    public void setUp() {

        ScriptEngineManager mgr = new ScriptEngineManager();
        engine = mgr.getEngineByExtension("vtl");

        spark = SparkSession.builder()
                .appName("test")
                .master("local")
                .getOrCreate();
        SparkSession.setActiveSession(spark);

        engine.put(VtlScriptEngine.PROCESSING_ENGINE_NAMES, "spark");
    }

    @AfterEach
    public void tearDown() {
        if (spark != null)
            spark.close();
    }

    @Test
    public void methodNonSerializableTODELETE() throws ScriptException {
        ScriptContext context = engine.getContext();
        context.setAttribute("ds1", DatasetSamples.ds1, ScriptContext.ENGINE_SCOPE);
        context.setAttribute("ds2", DatasetSamples.ds2, ScriptContext.ENGINE_SCOPE);

        engine.eval("ds_1 := ds1[keep id, long1]; ds_2 := ds2[keep id, long1]; " +
                "ds := ds_1 - ds_2;");
        List<Map<String, Object>> ds = ((Dataset) context.getAttribute("ds")).getDataAsMap();
    }

    @Test
    public void testOperators() throws ScriptException {

        ScriptContext context = engine.getContext();
        context.setAttribute("ds1", DatasetSamples.ds1, ScriptContext.ENGINE_SCOPE);
        context.setAttribute("ds2", DatasetSamples.ds2, ScriptContext.ENGINE_SCOPE);

        engine.eval("res := ds1#long1; " +
                "res := isnull(ds1); " +
                "ds_1 := ds1[keep id, long1, double1]; ds_2 := ds2[keep id, long1, double1]; " +
                "res := ds_1 + ds_2; " +
                "res := ds_1 - ds_2; " +
                "res := ds_1 * ds_2; " +
                "res := ds_1 / ds_2; " +
                "res := ds_1 = ds_2; " +
                "res := ds_1 <> ds_2; " +
                "res := ds_1 < ds_2; " +
                "res := ds_1 <= ds_2; " +
                "res := ds_1 > ds_2; " +
                "res := ds_1 >= ds_2; " +
                "res := + ds_1; " +
                "res := - ds_1; " +
                "res := ceil(floor(ln(exp(abs(ds_1))))); " +
                "res := round(ds_1, 5); " +
                "res := trunc(ds_1, 5); " +
                "res := sqrt(abs(ds_1)); " +
                "res := mod(ds_1, 5); " +
                "res := power(ds_1, 5); " +
                "res := log(abs(ds_1), 5); " +
                "ds_1 := ds1[keep id, string1, string2]; ds_2 := ds2[keep id, string1][calc string2 := string1]; " +
                "res := ds_1 || ds_2; "
        );
    }

    @Test
    public void testPlan() throws ScriptException {
        engine.getContext().setAttribute("ds_1", DatasetSamples.ds1, ScriptContext.ENGINE_SCOPE);
        engine.getContext().setAttribute("ds_2", DatasetSamples.ds2, ScriptContext.ENGINE_SCOPE);
        engine.eval("ds1 := ds_1[keep id, long1][rename long1 to bool_var]; " +
                "ds2 := ds_2[keep id, long1][rename long1 to bool_var]; " +
                "res := if ds1 > ds2 then ds1 else ds2;");
        var res = engine.getContext().getAttribute("res");
        assertThat(((Dataset) res).getDataAsMap()).containsExactlyInAnyOrder(
                Map.of("id", "Hadrien", "bool_var", 150L),
                Map.of("id", "Nico", "bool_var", 20L),
                Map.of("id", "Franck", "bool_var", 100L)
        );
        assertThat(((Dataset) res).getDataStructure().get("bool_var").getType()).isEqualTo(Long.class);
    }
}
