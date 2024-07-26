package fr.insee.vtl.spark.processing.engine;

import fr.insee.vtl.engine.VtlScriptEngine;
import fr.insee.vtl.model.utils.Java8Helpers;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.InMemoryDataset;
import fr.insee.vtl.model.Structured;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import static org.assertj.core.api.Assertions.assertThat;

public class CalcTest {

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

    InMemoryDataset dataset = new InMemoryDataset(
            Java8Helpers.listOf(
                    Java8Helpers.mapOf("name", "Hadrien", "age", 10L, "weight", 11L),
                    Java8Helpers.mapOf("name", "Nico", "age", 11L, "weight", 10L),
                    Java8Helpers.mapOf("name", "Franck", "age", 12L, "weight", 9L)
            ),
            Java8Helpers.mapOf("name", String.class, "age", Long.class, "weight", Long.class),
            Java8Helpers.mapOf("name", Dataset.Role.IDENTIFIER, "age", Dataset.Role.MEASURE, "weight", Dataset.Role.MEASURE)
    );

    @Test
    public void testCalcClause() throws ScriptException, InterruptedException {
        ScriptContext context = engine.getContext();
        context.setAttribute("ds1", dataset, ScriptContext.ENGINE_SCOPE);

        engine.eval("ds := ds1[calc test := between(age, 10, 11), age := age * 2, attribute wisdom := (weight + age) / 2];");

        Dataset ds = (Dataset) engine.getContext().getAttribute("ds");
        assertThat(ds).isInstanceOf(Dataset.class);
        assertThat(ds.getDataAsMap()).isEqualTo(Java8Helpers.listOf(
                Java8Helpers.mapOf("name", "Hadrien", "age", 20L, "test", true, "weight", 11L, "wisdom", 10.5D),
                Java8Helpers.mapOf("name", "Nico", "age", 22L, "test", true, "weight", 10L, "wisdom", 10.5D),
                Java8Helpers.mapOf("name", "Franck", "age", 24L, "test", false, "weight", 9L, "wisdom", 10.5D)
        ));
        assertThat(ds.getDataStructure()).containsValues(
                new Structured.Component("name", String.class, Dataset.Role.IDENTIFIER),
                new Structured.Component("age", Long.class, Dataset.Role.MEASURE),
                new Structured.Component("test", Boolean.class, Dataset.Role.MEASURE),
                new Structured.Component("weight", Long.class, Dataset.Role.MEASURE),
                new Structured.Component("wisdom", Double.class, Dataset.Role.ATTRIBUTE)
        );

    }
}
