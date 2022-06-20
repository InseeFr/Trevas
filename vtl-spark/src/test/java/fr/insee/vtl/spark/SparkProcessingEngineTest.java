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

    private final InMemoryDataset dataset1 = new InMemoryDataset(
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
    private final InMemoryDataset dataset2 = new InMemoryDataset(
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

    private final InMemoryDataset dataset3 = new InMemoryDataset(
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
    public void testCalcClause() throws ScriptException, InterruptedException {

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

        engine.eval("ds := ds1[calc test := between(age, 10, 11), age := age * 2, attribute wisdom := (weight + age) / 2];");

        var ds = (Dataset) engine.getContext().getAttribute("ds");
        assertThat(ds).isInstanceOf(Dataset.class);
        assertThat(ds.getDataAsMap()).isEqualTo(List.of(
                Map.of("name", "Hadrien", "age", 20L, "test", true, "weight", 11L, "wisdom", 10.5D),
                Map.of("name", "Nico", "age", 22L, "test", true, "weight", 10L, "wisdom", 10.5D),
                Map.of("name", "Franck", "age", 24L, "test", false, "weight", 9L, "wisdom", 10.5D)
        ));
        assertThat(ds.getDataStructure()).containsValues(
                new Component("name", String.class, Role.IDENTIFIER),
                new Component("age", Long.class, Role.MEASURE),
                new Component("test", Boolean.class, Role.MEASURE),
                new Component("weight", Long.class, Role.MEASURE),
                new Component("wisdom", Double.class, Role.ATTRIBUTE)
        );

    }

    @Test
    public void testLeftJoin() throws ScriptException {

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
    public void testInnerJoin() throws ScriptException {
        ScriptContext context = engine.getContext();
        context.getBindings(ScriptContext.ENGINE_SCOPE).put("ds1", dataset1);
        context.getBindings(ScriptContext.ENGINE_SCOPE).put("ds2", dataset2);
        context.getBindings(ScriptContext.ENGINE_SCOPE).put("ds3", dataset3);

        engine.eval("result := inner_join(ds1 as dsOne, ds2, ds3);");

        var resultInner = (Dataset) context.getAttribute("result");
        assertThat(resultInner.getDataAsList()).containsExactlyInAnyOrder(
                Arrays.asList("a", 1L, 2L, 9L, 10L, 16L, 17L),
                Arrays.asList("b", 3L, 4L, 11L, 12L, 18L, 19L),
                Arrays.asList("c", 5L, 6L, 12L, 13L, 20L, 21L),
                Arrays.asList("c", 5L, 6L, 12L, 13L, 22L, 23L),
                Arrays.asList("c", 5L, 6L, 14L, 15L, 20L, 21L),
                Arrays.asList("c", 5L, 6L, 14L, 15L, 22L, 23L)
        );

        assertThat(resultInner.getDataStructure()).containsValues(
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
    public void testFullJoin() throws ScriptException {
        ScriptContext context = engine.getContext();

        var ds1 = new InMemoryDataset(
                List.of(
                        new Component("id", String.class, Role.IDENTIFIER),
                        new Component("m1", Long.class, Role.MEASURE)
                ),
                Arrays.asList("b", 1L),
                Arrays.asList("c", 2L),
                Arrays.asList("d", 3L)
        );

        var ds2 = new InMemoryDataset(
                List.of(
                        new Component("id", String.class, Role.IDENTIFIER),
                        new Component("m1", Long.class, Role.MEASURE)
                ),
                Arrays.asList("a", 4L),
                Arrays.asList("b", 5L),
                Arrays.asList("c", 6L)
        );

        var ds3 = new InMemoryDataset(
                List.of(
                        new Component("id", String.class, Role.IDENTIFIER),
                        new Component("m1", Long.class, Role.MEASURE)
                ),
                Arrays.asList("a", 7L),
                Arrays.asList("d", 8L)
        );

        context.getBindings(ScriptContext.ENGINE_SCOPE).put("ds1", ds1);
        context.getBindings(ScriptContext.ENGINE_SCOPE).put("ds2", ds2);
        context.getBindings(ScriptContext.ENGINE_SCOPE).put("ds3", ds3);

        engine.eval("result := full_join(ds1 as dsOne, ds2, ds3);");

        var result = (Dataset) context.getAttribute("result");

        assertThat(result.getDataStructure().values()).containsExactly(
                new Component("id", String.class, Role.IDENTIFIER),
                new Component("dsOne#m1", Long.class, Role.MEASURE),
                new Component("ds2#m1", Long.class, Role.MEASURE),
                new Component("ds3#m1", Long.class, Role.MEASURE)
        );

        assertThat(result.getDataAsList()).containsExactlyInAnyOrder(
                Arrays.asList("d", 3L, null, 8L),
                Arrays.asList("c", 2L, 6L, null),
                Arrays.asList("b", 1L, 5L, null),
                Arrays.asList("a", null, 4L, 7L)
        );

    }

    @Test
    public void testCrossJoin() throws ScriptException {
        ScriptContext context = engine.getContext();
        context.getBindings(ScriptContext.ENGINE_SCOPE).put("ds1", dataset1);
        context.getBindings(ScriptContext.ENGINE_SCOPE).put("ds2", dataset2);
        context.getBindings(ScriptContext.ENGINE_SCOPE).put("ds3", dataset3);

        engine.eval("result := cross_join(ds1 as dsOne, ds2, ds3);");

        var resultCross = (Dataset) context.getAttribute("result");
        assertThat(resultCross.getDataAsList()).containsExactlyInAnyOrder(
                Arrays.asList("a", 1L, 2L, 9L, "a", 10L, 16L, "a", 17L),
                Arrays.asList("a", 1L, 2L, 9L, "a", 10L, 18L, "b", 19L),
                Arrays.asList("a", 1L, 2L, 9L, "a", 10L, 20L, "c", 21L),
                Arrays.asList("a", 1L, 2L, 9L, "a", 10L, 22L, "c", 23L),
                Arrays.asList("a", 1L, 2L, 11L, "b", 12L, 16L, "a", 17L),
                Arrays.asList("a", 1L, 2L, 11L, "b", 12L, 18L, "b", 19L),
                Arrays.asList("a", 1L, 2L, 11L, "b", 12L, 20L, "c", 21L),
                Arrays.asList("a", 1L, 2L, 11L, "b", 12L, 22L, "c", 23L),
                Arrays.asList("a", 1L, 2L, 12L, "c", 13L, 16L, "a", 17L),
                Arrays.asList("a", 1L, 2L, 12L, "c", 13L, 18L, "b", 19L),
                Arrays.asList("a", 1L, 2L, 12L, "c", 13L, 20L, "c", 21L),
                Arrays.asList("a", 1L, 2L, 12L, "c", 13L, 22L, "c", 23L),
                Arrays.asList("a", 1L, 2L, 14L, "c", 15L, 16L, "a", 17L),
                Arrays.asList("a", 1L, 2L, 14L, "c", 15L, 18L, "b", 19L),
                Arrays.asList("a", 1L, 2L, 14L, "c", 15L, 20L, "c", 21L),
                Arrays.asList("a", 1L, 2L, 14L, "c", 15L, 22L, "c", 23L),
                Arrays.asList("b", 3L, 4L, 9L, "a", 10L, 16L, "a", 17L),
                Arrays.asList("b", 3L, 4L, 9L, "a", 10L, 18L, "b", 19L),
                Arrays.asList("b", 3L, 4L, 9L, "a", 10L, 20L, "c", 21L),
                Arrays.asList("b", 3L, 4L, 9L, "a", 10L, 22L, "c", 23L),
                Arrays.asList("b", 3L, 4L, 11L, "b", 12L, 16L, "a", 17L),
                Arrays.asList("b", 3L, 4L, 11L, "b", 12L, 18L, "b", 19L),
                Arrays.asList("b", 3L, 4L, 11L, "b", 12L, 20L, "c", 21L),
                Arrays.asList("b", 3L, 4L, 11L, "b", 12L, 22L, "c", 23L),
                Arrays.asList("b", 3L, 4L, 12L, "c", 13L, 16L, "a", 17L),
                Arrays.asList("b", 3L, 4L, 12L, "c", 13L, 18L, "b", 19L),
                Arrays.asList("b", 3L, 4L, 12L, "c", 13L, 20L, "c", 21L),
                Arrays.asList("b", 3L, 4L, 12L, "c", 13L, 22L, "c", 23L),
                Arrays.asList("b", 3L, 4L, 14L, "c", 15L, 16L, "a", 17L),
                Arrays.asList("b", 3L, 4L, 14L, "c", 15L, 18L, "b", 19L),
                Arrays.asList("b", 3L, 4L, 14L, "c", 15L, 20L, "c", 21L),
                Arrays.asList("b", 3L, 4L, 14L, "c", 15L, 22L, "c", 23L),
                Arrays.asList("c", 5L, 6L, 9L, "a", 10L, 16L, "a", 17L),
                Arrays.asList("c", 5L, 6L, 9L, "a", 10L, 18L, "b", 19L),
                Arrays.asList("c", 5L, 6L, 9L, "a", 10L, 20L, "c", 21L),
                Arrays.asList("c", 5L, 6L, 9L, "a", 10L, 22L, "c", 23L),
                Arrays.asList("c", 5L, 6L, 11L, "b", 12L, 16L, "a", 17L),
                Arrays.asList("c", 5L, 6L, 11L, "b", 12L, 18L, "b", 19L),
                Arrays.asList("c", 5L, 6L, 11L, "b", 12L, 20L, "c", 21L),
                Arrays.asList("c", 5L, 6L, 11L, "b", 12L, 22L, "c", 23L),
                Arrays.asList("c", 5L, 6L, 12L, "c", 13L, 16L, "a", 17L),
                Arrays.asList("c", 5L, 6L, 12L, "c", 13L, 18L, "b", 19L),
                Arrays.asList("c", 5L, 6L, 12L, "c", 13L, 20L, "c", 21L),
                Arrays.asList("c", 5L, 6L, 12L, "c", 13L, 22L, "c", 23L),
                Arrays.asList("c", 5L, 6L, 14L, "c", 15L, 16L, "a", 17L),
                Arrays.asList("c", 5L, 6L, 14L, "c", 15L, 18L, "b", 19L),
                Arrays.asList("c", 5L, 6L, 14L, "c", 15L, 20L, "c", 21L),
                Arrays.asList("c", 5L, 6L, 14L, "c", 15L, 22L, "c", 23L),
                Arrays.asList("d", 7L, 8L, 9L, "a", 10L, 16L, "a", 17L),
                Arrays.asList("d", 7L, 8L, 9L, "a", 10L, 18L, "b", 19L),
                Arrays.asList("d", 7L, 8L, 9L, "a", 10L, 20L, "c", 21L),
                Arrays.asList("d", 7L, 8L, 9L, "a", 10L, 22L, "c", 23L),
                Arrays.asList("d", 7L, 8L, 11L, "b", 12L, 16L, "a", 17L),
                Arrays.asList("d", 7L, 8L, 11L, "b", 12L, 18L, "b", 19L),
                Arrays.asList("d", 7L, 8L, 11L, "b", 12L, 20L, "c", 21L),
                Arrays.asList("d", 7L, 8L, 11L, "b", 12L, 22L, "c", 23L),
                Arrays.asList("d", 7L, 8L, 12L, "c", 13L, 16L, "a", 17L),
                Arrays.asList("d", 7L, 8L, 12L, "c", 13L, 18L, "b", 19L),
                Arrays.asList("d", 7L, 8L, 12L, "c", 13L, 20L, "c", 21L),
                Arrays.asList("d", 7L, 8L, 12L, "c", 13L, 22L, "c", 23L),
                Arrays.asList("d", 7L, 8L, 14L, "c", 15L, 16L, "a", 17L),
                Arrays.asList("d", 7L, 8L, 14L, "c", 15L, 18L, "b", 19L),
                Arrays.asList("d", 7L, 8L, 14L, "c", 15L, 20L, "c", 21L),
                Arrays.asList("d", 7L, 8L, 14L, "c", 15L, 22L, "c", 23L)
        );

        assertThat(resultCross.getDataStructure().values()).containsExactly(
                new Component("dsOne#name", String.class, Role.IDENTIFIER),
                new Component("age", Long.class, Role.MEASURE),
                new Component("weight", Long.class, Role.MEASURE),
                new Component("age2", Long.class, Role.MEASURE),
                new Component("ds2#name", String.class, Role.IDENTIFIER),
                new Component("weight2", Long.class, Role.MEASURE),
                new Component("age3", Long.class, Role.MEASURE),
                new Component("ds3#name", String.class, Role.IDENTIFIER),
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
    public void testAggregateClause() throws ScriptException {

        InMemoryDataset dataset = new InMemoryDataset(
                List.of(
                        Map.of("name", "Hadrien", "country", "norway", "age", 10L, "weight", 11D),
                        Map.of("name", "Nico", "country", "france", "age", 11L, "weight", 10D),
                        Map.of("name", "Franck", "country", "france", "age", 12L, "weight", 9D),
                        Map.of("name", "pengfei", "country", "france", "age", 13L, "weight", 11D)
                ),
                Map.of("name", String.class, "country", String.class, "age", Long.class, "weight", Double.class),
                Map.of("name", Role.IDENTIFIER, "country", Role.IDENTIFIER, "age", Role.MEASURE, "weight", Role.MEASURE)
        );

        ScriptContext context = engine.getContext();
        context.setAttribute("ds1", dataset, ScriptContext.ENGINE_SCOPE);


        engine.eval("res := ds1[aggr " +
                "sumAge := sum(age*2)," +
                "avgWeight := avg(weight)," +
                "countVal := count()," +
                "maxAge := max(age)," +
                "maxWeight := max(weight)," +
                "minAge := min(age)," +
                "minWeight := min(weight)," +
                "medianAge := median(age)," +
                "medianWeight := median(weight)" +
                " group by country];");
        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);
        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
                Map.of("country", "france", "sumAge", 72L, "avgWeight", 10.0D,
                        "countVal", 3L, "maxAge", 13L, "maxWeight", 11.0D,
                        "minAge", 11L, "minWeight", 9D, "medianAge", 12L,
                        "medianWeight", 10.0D),
                Map.of("country", "norway", "sumAge", 20L, "avgWeight", 11.0,
                        "countVal", 1L, "maxAge", 10L, "maxWeight", 11.0D,
                        "minAge", 10L, "minWeight", 11D, "medianAge", 10L,
                        "medianWeight", 11D)
        );

//        InMemoryDataset dataset2 = new InMemoryDataset(
//                List.of(
//                        Map.of("name", "Hadrien", "country", "norway", "age", 10L, "weight", 11D),
//                        Map.of("name", "Nico", "country", "france", "age", 9L, "weight", 5D),
//                        Map.of("name", "Franck", "country", "france", "age", 10L, "weight", 15D),
//                        Map.of("name", "Nico1", "country", "france", "age", 11L, "weight", 10D),
//                        Map.of("name", "Franck1", "country", "france", "age", 12L, "weight", 8D)
//                ),
//                Map.of("name", String.class, "country", String.class, "age", Long.class, "weight", Double.class),
//                Map.of("name", Role.IDENTIFIER, "country", Role.IDENTIFIER, "age", Role.MEASURE, "weight", Role.MEASURE)
//        );
//
//        context.setAttribute("ds2", dataset2, ScriptContext.ENGINE_SCOPE);
//
//        engine.eval("res := ds2[aggr " +
//                "stddev_popAge := stddev_pop(age), " +
//                "stddev_popWeight := stddev_pop(weight), " +
//                "stddev_sampAge := stddev_samp(age), " +
//                "stddev_sampWeight := stddev_samp(weight), " +
//                "var_popAge := var_pop(age), " +
//                "var_popWeight := var_pop(weight), " +
//                "var_sampAge := var_samp(age), " +
//                "var_sampWeight := var_samp(weight)" +
//                " group by country];");
//
//        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);
//
//        var fr = ((Dataset) engine.getContext().getAttribute("res")).getDataAsMap().get(0);
//
//        assertThat((Double) fr.get("stddev_popAge")).isCloseTo(1.118, Percentage.withPercentage(2));
//        assertThat((Double) fr.get("stddev_popWeight")).isCloseTo(3.640, Percentage.withPercentage(2));
//        assertThat((Double) fr.get("stddev_sampAge")).isCloseTo(1.290, Percentage.withPercentage(2));
//        assertThat((Double) fr.get("stddev_sampWeight")).isCloseTo(4.2, Percentage.withPercentage(2));
//        assertThat((Double) fr.get("var_popAge")).isEqualTo(1.25);
//        assertThat((Double) fr.get("var_popWeight")).isEqualTo(13.25);
//        assertThat((Double) fr.get("var_sampAge")).isCloseTo(1.666, Percentage.withPercentage(2));
//        assertThat((Double) fr.get("var_sampWeight")).isCloseTo(17.666, Percentage.withPercentage(2));
//
//        var no = ((Dataset) engine.getContext().getAttribute("res")).getDataAsMap().get(1);
//
//        assertThat((Double) no.get("stddev_popAge")).isEqualTo(0.0);
//        assertThat((Double) no.get("stddev_popWeight")).isEqualTo(0.0);
//        assertThat((Double) no.get("stddev_sampAge")).isEqualTo(0.0);
//        assertThat((Double) no.get("stddev_sampWeight")).isEqualTo(0.0);
//        assertThat((Double) no.get("var_popAge")).isEqualTo(0.0);
//        assertThat((Double) no.get("var_popWeight")).isEqualTo(0.0);
//        assertThat((Double) no.get("var_sampAge")).isEqualTo(0.0);
//        assertThat((Double) no.get("var_sampWeight")).isEqualTo(0.0);

    }
    @Test
    public void testAnClause() throws ScriptException {

        InMemoryDataset dataset = new InMemoryDataset(
                List.of(
                        Map.of("name", "Hadrien", "country", "norway", "age", 10L, "weight", 11D),
                        Map.of("name", "Nico", "country", "france", "age", 11L, "weight", 10D),
                        Map.of("name", "Franck", "country", "france", "age", 12L, "weight", 9D),
                        Map.of("name", "pengfei", "country", "france", "age", 13L, "weight", 11D)
                ),
                Map.of("name", String.class, "country", String.class, "age", Long.class, "weight", Double.class),
                Map.of("name", Role.IDENTIFIER, "country", Role.IDENTIFIER, "age", Role.MEASURE, "weight", Role.MEASURE)
        );

        ScriptContext context = engine.getContext();
        context.setAttribute("ds1", dataset, ScriptContext.ENGINE_SCOPE);


        engine.eval("res := count ( ds1 over ( partition by country ) )");
        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);
        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
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