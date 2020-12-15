package fr.insee.vtl.spark;

import fr.insee.vtl.engine.VtlScriptEngine;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.Dataset.Role;
import fr.insee.vtl.model.InMemoryDataset;
import fr.insee.vtl.model.ProcessingEngineFactory;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

import static fr.insee.vtl.model.Structured.Component;
import static org.assertj.core.api.Assertions.assertThat;

public class SparkProcessingEngineTest {

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

    public void testLoadActiveSession() {
        SparkSession spark = SparkSession.builder()
                .appName("test")
                .master("local")
                .getOrCreate();
        SparkSession.setActiveSession(spark);
        try {

        } finally {
            spark.close();
        }
    }

    @Test
    public void testServiceLoader() {
        List<String> processingEngines = ServiceLoader.load(ProcessingEngineFactory.class).stream()
                .map(ServiceLoader.Provider::get)
                .map(ProcessingEngineFactory::getName)
                .collect(Collectors.toList());
        assertThat(processingEngines).containsExactlyInAnyOrder(
                "memory", "spark"
        );
    }

    @Test
    public void testCalcClause() throws ScriptException {

        InMemoryDataset dataset = new InMemoryDataset(
                List.of(
                        Map.of("name", "Hadrien", "age", 10L, "weight", 11L),
                        Map.of("name", "Nico", "age", 11L, "weight", 10L),
                        Map.of("name", "Franck", "age", 12L, "weight", 9L)
                ),
                Map.of("name", String.class, "age", Long.class, "weight", Long.class),
                Map.of("name", Role.IDENTIFIER, "age", Role.MEASURE, "weight", Role.MEASURE)
        );

        ScriptContext context = engine.getContext();
        context.setAttribute("ds1", dataset, ScriptContext.ENGINE_SCOPE);

        engine.eval("ds := ds1[calc age := age * 2, attribute wisdom := (weight + age) / 2];");

        var ds = (Dataset) engine.getContext().getAttribute("ds");
        assertThat(ds).isInstanceOf(Dataset.class);
        assertThat(ds.getDataAsMap()).isEqualTo(List.of(
                Map.of("name", "Hadrien", "age", 20L, "weight", 11L, "wisdom", 10.5D),
                Map.of("name", "Nico", "age", 22L, "weight", 10L, "wisdom", 10.5D),
                Map.of("name", "Franck", "age", 24L, "weight", 9L, "wisdom", 10.5D)
        ));
        assertThat(ds.getDataStructure()).containsValues(
                new Component("name", String.class, Role.IDENTIFIER),
                new Component("age", Long.class, Role.MEASURE),
                new Component("weight", Long.class, Role.MEASURE),
                new Component("wisdom", Double.class, Role.ATTRIBUTE)
        );

    }

    @Test
    public void testJoinWithAlias() throws ScriptException {

        InMemoryDataset dataset1 = new InMemoryDataset(
                List.of(
                        List.of("a", 1L, 2L),
                        List.of("b", 3L, 4L),
                        List.of("c", 5L, 6L),
                        List.of("d", 7L, 8L)
                ),
                List.of(
                        new Component("name", String.class, Role.IDENTIFIER),
                        new Component("age", Long.class, Role.MEASURE),
                        new Component("weight", Long.class, Role.MEASURE)
                )
        );
        InMemoryDataset dataset2 = new InMemoryDataset(
                List.of(
                        List.of(9L, "a", 10L),
                        List.of(11L, "b", 12L),
                        List.of(12L, "c", 13L),
                        List.of(14L, "c", 15L)
                ),
                List.of(
                        new Component("age2", Long.class, Role.MEASURE),
                        new Component("name", String.class, Role.IDENTIFIER),
                        new Component("weight2", Long.class, Role.MEASURE)
                )
        );

        InMemoryDataset dataset3 = new InMemoryDataset(
                List.of(
                        List.of(16L, "a", 17L),
                        List.of(18L, "b", 19L),
                        List.of(20L, "c", 21L),
                        List.of(22L, "c", 23L)
                ),
                List.of(
                        new Component("age3", Long.class, Role.MEASURE),
                        new Component("name", String.class, Role.IDENTIFIER),
                        new Component("weight3", Long.class, Role.MEASURE)
                )
        );

        ScriptContext context = engine.getContext();
        context.getBindings(ScriptContext.ENGINE_SCOPE).put("ds1", dataset1);
        context.getBindings(ScriptContext.ENGINE_SCOPE).put("ds2", dataset2);
        context.getBindings(ScriptContext.ENGINE_SCOPE).put("ds3", dataset3);

        engine.eval("result := left_join(ds1 as dsOne, ds2, ds3);");

        var result = (Dataset) context.getAttribute("result");
        assertThat(result.getDataAsList()).containsExactlyInAnyOrder(
                Arrays.asList("a", 1L, 2L, 9L, 10L, 16L, 17L),
                Arrays.asList("b", 3L, 4L, 11L, 12L, 18L, 19L),
                Arrays.asList("c", 5L, 6L, 12L, 13L, 20L, 21L),
                Arrays.asList("c", 5L, 6L, 12L, 13L, 22L, 23L),
                Arrays.asList("c", 5L, 6L, 14L, 15L, 20L, 21L),
                Arrays.asList("c", 5L, 6L, 14L, 15L, 22L, 23L),
                Arrays.asList("d", 7L, 8L, null, null, null, null)
        );

        assertThat(result.getDataStructure()).containsValues(
                new Component("name", String.class, Role.IDENTIFIER),
                new Component("age", Long.class, Role.MEASURE),
                new Component("weight", Long.class, Role.MEASURE),
                new Component("age2", Long.class, Role.MEASURE),
                new Component("weight2", Long.class, Role.MEASURE),
                new Component("age3", Long.class, Role.MEASURE),
                new Component("weight3", Long.class, Role.MEASURE)
        );
    }

    @Test
    public void testFilterClause() throws ScriptException {

        InMemoryDataset dataset = new InMemoryDataset(
                List.of(
                        Map.of("name", "Hadrien", "age", 10L, "weight", 11L),
                        Map.of("name", "Nico", "age", 11L, "weight", 10L),
                        Map.of("name", "Franck", "age", 12L, "weight", 9L)
                ),
                Map.of("name", String.class, "age", Long.class, "weight", Long.class),
                Map.of("name", Role.IDENTIFIER, "age", Role.MEASURE, "weight", Role.MEASURE)
        );

        ScriptContext context = engine.getContext();
        context.setAttribute("ds1", dataset, ScriptContext.ENGINE_SCOPE);

        engine.eval("ds := ds1[filter age > 10 and age < 12];");

        assertThat(engine.getContext().getAttribute("ds")).isInstanceOf(Dataset.class);
        var ds = (Dataset) engine.getContext().getAttribute("ds");
        assertThat(ds.getDataAsMap()).isEqualTo(List.of(
                Map.of("name", "Nico", "age", 11L, "weight", 10L)
        ));

        assertThat(ds.getDataStructure()).containsValues(
                new Component("name", String.class, Role.IDENTIFIER),
                new Component("age", Long.class, Role.MEASURE),
                new Component("weight", Long.class, Role.MEASURE)
        );


    }

    @Test
    public void testRename() throws ScriptException {
        InMemoryDataset dataset = new InMemoryDataset(
                List.of(
                        Map.of("name", "Hadrien", "age", 10L, "weight", 11L),
                        Map.of("name", "Nico", "age", 11L, "weight", 10L),
                        Map.of("name", "Franck", "age", 12L, "weight", 9L)
                ),
                Map.of("name", String.class, "age", Long.class, "weight", Long.class),
                Map.of("name", Role.IDENTIFIER, "age", Role.MEASURE, "weight", Role.MEASURE)
        );

        ScriptContext context = engine.getContext();
        context.setAttribute("ds1", dataset, ScriptContext.ENGINE_SCOPE);

        engine.eval("ds := ds1[rename age to weight, weight to age, name to pseudo];");

        assertThat(engine.getContext().getAttribute("ds")).isInstanceOf(Dataset.class);
        var ds = (Dataset) engine.getContext().getAttribute("ds");
        assertThat(ds.getDataAsMap()).containsExactlyInAnyOrder(
                Map.of("pseudo", "Hadrien", "weight", 10L, "age", 11L),
                Map.of("pseudo", "Nico", "weight", 11L, "age", 10L),
                Map.of("pseudo", "Franck", "weight", 12L, "age", 9L)
        );
        assertThat(ds.getDataStructure()).containsValues(
                new Component("pseudo", String.class, Role.IDENTIFIER),
                new Component("age", Long.class, Role.MEASURE),
                new Component("weight", Long.class, Role.MEASURE)
        );
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
                Map.of("name", Role.IDENTIFIER, "age", Role.MEASURE, "weight", Role.MEASURE)
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
        var ds = (Dataset) engine.getContext().getAttribute("ds");
        assertThat(ds.getDataAsMap()).containsExactly(
                Map.of("name", "Hadrien", "age", 10L),
                Map.of("name", "Nico", "age", 11L),
                Map.of("name", "Franck", "age", 12L)
        );
        assertThat(ds.getDataStructure()).containsValues(
                new Component("name", String.class, Role.IDENTIFIER),
                new Component("age", Long.class, Role.MEASURE)
        );
    }
}