package fr.insee.vtl.spark;

import fr.insee.vtl.engine.VtlScriptEngine;
import fr.insee.vtl.engine.VtlScriptEngineFactory;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.InMemoryDataset;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.script.ScriptContext;
import javax.script.ScriptException;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class SparkProcessingEngineTest {

    private SparkSession spark;
    private VtlScriptEngine engine;

    @BeforeEach
    public void setUp() {
        engine = new VtlScriptEngine(new VtlScriptEngineFactory());

        spark = SparkSession.builder()
                .appName("test")
                .master("local")
                .getOrCreate();

        engine.setProcessingEngine(new SparkProcessingEngine(spark));
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        Thread.sleep(1000000);
        spark.close();
    }

    @Test
    public void testProjection() throws ScriptException {

        InMemoryDataset dataset = new InMemoryDataset(
                List.of(
                        Map.of("name", "Hadrien", "age", 10L, "weight", 11L),
                        Map.of("name", "Nico", "age", 11L, "weight", 10L),
                        Map.of("name", "Franck", "age", 12L, "weight", 9L)
                ),
                Map.of("name", String.class, "age", Long.class, "weight", Long.class),
                Map.of("name", fr.insee.vtl.model.Dataset.Role.IDENTIFIER, "age", fr.insee.vtl.model.Dataset.Role.MEASURE, "weight", fr.insee.vtl.model.Dataset.Role.MEASURE)
        );

        ScriptContext context = engine.getContext();
        context.setAttribute("ds1", dataset, ScriptContext.ENGINE_SCOPE);

        engine.eval("ds := ds1[keep name, age];");

        assertThat(engine.getContext().getAttribute("ds")).isInstanceOf(fr.insee.vtl.model.Dataset.class);
        assertThat(((fr.insee.vtl.model.Dataset) engine.getContext().getAttribute("ds")).getDataAsMap()).containsExactly(
                Map.of("name", "Hadrien", "age", 10L),
                Map.of("name", "Nico", "age", 11L),
                Map.of("name", "Franck", "age", 12L)
        );

        engine.eval("ds := ds1[drop weight];");

        assertThat(engine.getContext().getAttribute("ds")).isInstanceOf(fr.insee.vtl.model.Dataset.class);
        assertThat(((Dataset) engine.getContext().getAttribute("ds")).getDataAsMap()).containsExactly(
                Map.of("name", "Hadrien", "age", 10L),
                Map.of("name", "Nico", "age", 11L),
                Map.of("name", "Franck", "age", 12L)
        );
    }
}