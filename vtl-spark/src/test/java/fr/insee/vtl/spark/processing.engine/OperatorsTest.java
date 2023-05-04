package fr.insee.vtl.spark.processing.engine;

import fr.insee.vtl.engine.VtlScriptEngine;
import fr.insee.vtl.spark.samples.DatasetSamples;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

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
                "ds_1 := ds1[keep id, string1, string2]; ds_2 := ds2[keep id, string1, string2]; " +
                "res := ds1 || ds2; "
        );
    }
}
