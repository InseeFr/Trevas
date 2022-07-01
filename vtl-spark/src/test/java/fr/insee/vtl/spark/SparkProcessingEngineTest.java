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

    private final InMemoryDataset anCountDS1  = new InMemoryDataset(
            List.of(
            Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 3L,"Me_2",1D),
                        Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 4L,"Me_2",9D),
                                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 7L,"Me_2",5D),
                                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 6L,"Me_2",8D),
                                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 9L,"Me_2",3D),
                                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 5L,"Me_2",4D),
                                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 10L,"Me_2",2D),
                                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 5L,"Me_2",7D)

                                ),
                                Map.of("Id_1", String.class, "Id_2", String.class, "Year", Long.class, "Me_1", Long.class,"Me_2",Double.class),
            Map.of("Id_1", Role.IDENTIFIER, "Id_2", Role.IDENTIFIER, "Year", Role.IDENTIFIER, "Me_1", Role.MEASURE,"Me_2", Role.MEASURE)
            );

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

    /*
    * Test case for analytic function Count
    *
    * */

    @Test
    public void testAnCountWithPartitionClause() throws ScriptException {

        // Analytical function Test case 1 : count on window with partition
        /* Input dataset
        *   +----+----+----+----+----+
            |Id_1|Id_2|Year|Me_1|Me_2|
            +----+----+----+----+----+
            |   A|  XX|2000|   3| 1.0|
            |   A|  XX|2001|   4| 9.0|
            |   A|  XX|2002|   7| 5.0|
            |   A|  XX|2003|   6| 8.0|
            |   A|  YY|2000|   9| 3.0|
            |   A|  YY|2001|   5| 4.0|
            |   A|  YY|2002|  10| 2.0|
            |   A|  YY|2003|   5| 7.0|
            +----+----+----+----+----+
        * */
        ScriptContext context = engine.getContext();
        context.setAttribute("ds1", anCountDS1 , ScriptContext.ENGINE_SCOPE);


        engine.eval("res := count ( ds1 over ( partition by Id_1 ) )");
        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);

        /*
        * The result data frame need to check mutable or not mutable on Mesaument column
        *
        *   +----+----+----+----+----+----------+----------+
            |Id_1|Id_2|Year|Me_1|Me_2|count_Me_1|count_Me_2|
            +----+----+----+----+----+----------+----------+
            |   A|  XX|2000|   3| 1.0|         8|         8|
            |   A|  XX|2001|   4| 9.0|         8|         8|
            |   A|  XX|2002|   7| 5.0|         8|         8|
            |   A|  XX|2003|   6| 8.0|         8|         8|
            |   A|  YY|2000|   9| 3.0|         8|         8|
            |   A|  YY|2001|   5| 4.0|         8|         8|
            |   A|  YY|2002|  10| 2.0|         8|         8|
            |   A|  YY|2003|   5| 7.0|         8|         8|
            +----+----+----+----+----+----------+----------+
        * */
        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 8L,"Me_2",8L),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 8L,"Me_2",8L),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 8L,"Me_2",8L),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 8L,"Me_2",8L),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 8L,"Me_2",8L),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 8L,"Me_2",8L),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 8L,"Me_2",8L),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 8L,"Me_2",8L)
        );

    }

    @Test
    public void testAnCountWithPartitionOrderByClause() throws ScriptException {

        // Analytical function Test case 2 : count on window with partition and orderBy
        /* Input dataset
        *   +----+----+----+----+----+
            |Id_1|Id_2|Year|Me_1|Me_2|
            +----+----+----+----+----+
            |   A|  XX|2000|   3| 1.0|
            |   A|  XX|2001|   4| 9.0|
            |   A|  XX|2002|   7| 5.0|
            |   A|  XX|2003|   6| 8.0|
            |   A|  YY|2000|   9| 3.0|
            |   A|  YY|2001|   5| 4.0|
            |   A|  YY|2002|  10| 2.0|
            |   A|  YY|2003|   5| 7.0|
            +----+----+----+----+----+
        * */
        ScriptContext context = engine.getContext();
        context.setAttribute("ds1", anCountDS1 , ScriptContext.ENGINE_SCOPE);


        engine.eval("res := count ( ds1 over ( partition by Id_1 order by Id_2) )");
        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);

        /*
        * The result data frame
        *
        *   +----+----+----+----+----+----------+----------+
            |Id_1|Id_2|Year|Me_1|Me_2|count_Me_1|count_Me_2|
            +----+----+----+----+----+----------+----------+
            |   A|  XX|2000|   3| 1.0|         4|         4|
            |   A|  XX|2001|   4| 9.0|         4|         4|
            |   A|  XX|2002|   7| 5.0|         4|         4|
            |   A|  XX|2003|   6| 8.0|         4|         4|
            |   A|  YY|2000|   9| 3.0|         8|         8|
            |   A|  YY|2001|   5| 4.0|         8|         8|
            |   A|  YY|2002|  10| 2.0|         8|         8|
            |   A|  YY|2003|   5| 7.0|         8|         8|
            +----+----+----+----+----+----------+----------+
        * */
        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 4L,"Me_2",4L),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 4L,"Me_2",4L),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 4L,"Me_2",4L),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 4L,"Me_2",4L),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 8L,"Me_2",8L),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 8L,"Me_2",8L),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 8L,"Me_2",8L),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 8L,"Me_2",8L)
        );

    }

    @Test
    public void testAnCountWithPartitionOrderByDPClause() throws ScriptException {

        // Analytical function count test case 3 : count on window with partition, orderBy and data points
        /* Input dataset
        *   +----+----+----+----+----+
            |Id_1|Id_2|Year|Me_1|Me_2|
            +----+----+----+----+----+
            |   A|  XX|2000|   3| 1.0|
            |   A|  XX|2001|   4| 9.0|
            |   A|  XX|2002|   7| 5.0|
            |   A|  XX|2003|   6| 8.0|
            |   A|  YY|2000|   9| 3.0|
            |   A|  YY|2001|   5| 4.0|
            |   A|  YY|2002|  10| 2.0|
            |   A|  YY|2003|   5| 7.0|
            +----+----+----+----+----+
        * */
        ScriptContext context = engine.getContext();
        context.setAttribute("ds1", anCountDS1 , ScriptContext.ENGINE_SCOPE);


        engine.eval("res := count ( ds1 over ( partition by Id_1 order by Id_2 data points between 2 preceding and 2 following) )");
        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);

        /*
        * The result data frame
        *
        *   +----+----+----+----+----+----------+----------+
            |Id_1|Id_2|Year|Me_1|Me_2|count_Me_1|count_Me_2|
            +----+----+----+----+----+----------+----------+
            |   A|  XX|2000|   3| 1.0|         3|         3|
            |   A|  XX|2001|   4| 9.0|         4|         4|
            |   A|  XX|2002|   7| 5.0|         5|         5|
            |   A|  XX|2003|   6| 8.0|         5|         5|
            |   A|  YY|2000|   9| 3.0|         5|         5|
            |   A|  YY|2001|   5| 4.0|         5|         5|
            |   A|  YY|2002|  10| 2.0|         4|         4|
            |   A|  YY|2003|   5| 7.0|         3|         3|
            +----+----+----+----+----+----------+----------+

        * */
        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 3L,"Me_2",3L),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 4L,"Me_2",4L),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 5L,"Me_2",5L),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 5L,"Me_2",5L),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 5L,"Me_2",5L),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 5L,"Me_2",5L),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 4L,"Me_2",4L),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 3L,"Me_2",3L)
        );

    }

    @Test
    public void testAnCountWithPartitionOrderByRangeClause() throws ScriptException {

        // Analytical function count test case 4 : count on window with partition, orderBy and range
        // Because range build window based on the ordered column value, and Id_2 is string, so we change
        // the order by column to year.
        /* Input dataset
        *   +----+----+----+----+----+
            |Id_1|Id_2|Year|Me_1|Me_2|
            +----+----+----+----+----+
            |   A|  XX|2000|   3| 1.0|
            |   A|  XX|2001|   4| 9.0|
            |   A|  XX|2002|   7| 5.0|
            |   A|  XX|2003|   6| 8.0|
            |   A|  YY|2000|   9| 3.0|
            |   A|  YY|2001|   5| 4.0|
            |   A|  YY|2002|  10| 2.0|
            |   A|  YY|2003|   5| 7.0|
            +----+----+----+----+----+
        * */
        ScriptContext context = engine.getContext();
        context.setAttribute("ds1", anCountDS1 , ScriptContext.ENGINE_SCOPE);


        engine.eval("res := count ( ds1 over ( partition by Id_1 order by Year range between -1 and 1) )");
        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);

        /*
        * The result data frame
        *
        *   +----+----+----+----+----+----------+----------+
            |Id_1|Id_2|Year|Me_1|Me_2|count_Me_1|count_Me_2|
            +----+----+----+----+----+----------+----------+
            |   A|  XX|2000|   3| 1.0|         4|         4|
            |   A|  YY|2000|   9| 3.0|         4|         4|
            |   A|  XX|2001|   4| 9.0|         6|         6|
            |   A|  YY|2001|   5| 4.0|         6|         6|
            |   A|  XX|2002|   7| 5.0|         6|         6|
            |   A|  YY|2002|  10| 2.0|         6|         6|
            |   A|  XX|2003|   6| 8.0|         4|         4|
            |   A|  YY|2003|   5| 7.0|         4|         4|
            +----+----+----+----+----+----------+----------+

        * */
        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 4L,"Me_2",4L),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 4L,"Me_2",4L),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 6L,"Me_2",6L),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 6L,"Me_2",6L),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 6L,"Me_2",6L),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 6L,"Me_2",6L),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 4L,"Me_2",4L),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 4L,"Me_2",4L)
        );

    }

    /*
    * End of count test case */


    /*
     * Test case for analytic function Sum
     *
     * */
    @Test
    public void testAnSumWithPartitionClause() throws ScriptException {

        // Analytical function Test case 1 : sum on window with partition
        /* Input dataset
        *   +----+----+----+----+----+
            |Id_1|Id_2|Year|Me_1|Me_2|
            +----+----+----+----+----+
            |   A|  XX|2000|   3| 1.0|
            |   A|  XX|2001|   4| 9.0|
            |   A|  XX|2002|   7| 5.0|
            |   A|  XX|2003|   6| 8.0|
            |   A|  YY|2000|   9| 3.0|
            |   A|  YY|2001|   5| 4.0|
            |   A|  YY|2002|  10| 2.0|
            |   A|  YY|2003|   5| 7.0|
            +----+----+----+----+----+
        * */
        ScriptContext context = engine.getContext();
        context.setAttribute("ds1", anCountDS1 , ScriptContext.ENGINE_SCOPE);


        engine.eval("res := sum ( ds1 over ( partition by Id_1, Id_2 ) )");
        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);

        /*
        * The result data frame need to check mutable or not mutable on Mesaument column
        *
        *   +----+----+----+----+----+----------+----------+
            |Id_1|Id_2|Year|Me_1|Me_2|sum_Me_1|sum_Me_2|
            +----+----+----+----+----+--------+--------+
            |   A|  XX|2000|   3| 1.0|      20|    23.0|
            |   A|  XX|2001|   4| 9.0|      20|    23.0|
            |   A|  XX|2002|   7| 5.0|      20|    23.0|
            |   A|  XX|2003|   6| 8.0|      20|    23.0|
            |   A|  YY|2000|   9| 3.0|      29|    16.0|
            |   A|  YY|2001|   5| 4.0|      29|    16.0|
            |   A|  YY|2002|  10| 2.0|      29|    16.0|
            |   A|  YY|2003|   5| 7.0|      29|    16.0|
            +----+----+----+----+----+--------+--------+
        * */
        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 20L,"Me_2",23.0D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 20L,"Me_2",23.0D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 20L,"Me_2",23.0D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 20L,"Me_2",23.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 29L,"Me_2",16.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 29L,"Me_2",16.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 29L,"Me_2",16.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 29L,"Me_2",16.0D)
        );

    }

    @Test
    public void testAnSumWithPartitionOrderByClause() throws ScriptException {

        // Analytical function Test case 2 : sum on window with partition and order by
        /* Input dataset
        *   +----+----+----+----+----+
            |Id_1|Id_2|Year|Me_1|Me_2|
            +----+----+----+----+----+
            |   A|  XX|2000|   3| 1.0|
            |   A|  XX|2001|   4| 9.0|
            |   A|  XX|2002|   7| 5.0|
            |   A|  XX|2003|   6| 8.0|
            |   A|  YY|2000|   9| 3.0|
            |   A|  YY|2001|   5| 4.0|
            |   A|  YY|2002|  10| 2.0|
            |   A|  YY|2003|   5| 7.0|
            +----+----+----+----+----+
        * */
        ScriptContext context = engine.getContext();
        context.setAttribute("ds1", anCountDS1 , ScriptContext.ENGINE_SCOPE);


        engine.eval("res := sum ( ds1 over ( partition by Id_1 order by Id_2) )");
        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);

        /*
        * The result data frame need to check mutable or not mutable on Mesaument column
        *
        *   +----+----+----+----+----+----------+----------+
            |Id_1|Id_2|Year|Me_1|Me_2|sum_Me_1|sum_Me_2|
            +----+----+----+----+----+--------+--------+
            |   A|  XX|2000|   3| 1.0|      20|    23.0|
            |   A|  XX|2001|   4| 9.0|      20|    23.0|
            |   A|  XX|2002|   7| 5.0|      20|    23.0|
            |   A|  XX|2003|   6| 8.0|      20|    23.0|
            |   A|  YY|2000|   9| 3.0|      49|    39.0|
            |   A|  YY|2001|   5| 4.0|      49|    39.0|
            |   A|  YY|2002|  10| 2.0|      49|    39.0|
            |   A|  YY|2003|   5| 7.0|      49|    39.0|
            +----+----+----+----+----+--------+--------+

        * */
        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 20L,"Me_2",23.0D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 20L,"Me_2",23.0D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 20L,"Me_2",23.0D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 20L,"Me_2",23.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 49L,"Me_2",39.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 49L,"Me_2",39.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 49L,"Me_2",39.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 49L,"Me_2",39.0D)
        );

    }

    @Test
    public void testAnSumWithOrderByClause() throws ScriptException {

        // Analytical function Test case 3 : sum on window with only order by without partition
        /* Input dataset
        *   +----+----+----+----+----+
            |Id_1|Id_2|Year|Me_1|Me_2|
            +----+----+----+----+----+
            |   A|  XX|2000|   3| 1.0|
            |   A|  XX|2001|   4| 9.0|
            |   A|  XX|2002|   7| 5.0|
            |   A|  XX|2003|   6| 8.0|
            |   A|  YY|2000|   9| 3.0|
            |   A|  YY|2001|   5| 4.0|
            |   A|  YY|2002|  10| 2.0|
            |   A|  YY|2003|   5| 7.0|
            +----+----+----+----+----+
        * */
        ScriptContext context = engine.getContext();
        context.setAttribute("ds1", anCountDS1 , ScriptContext.ENGINE_SCOPE);


        engine.eval("res := sum ( ds1 over ( order by Id_1, Id_2, Year ) )");
        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);

        /*
        * The result data frame need to check mutable or not mutable on Mesaument column
        *
        *   +----+----+----+----+----+----------+----------+
            |Id_1|Id_2|Year|Me_1|Me_2|sum_Me_1|sum_Me_2|
            +----+----+----+----+----+--------+--------+
            |   A|  XX|2000|   3| 1.0|       3|     1.0|
            |   A|  XX|2001|   4| 9.0|       7|    10.0|
            |   A|  XX|2002|   7| 5.0|      14|    15.0|
            |   A|  XX|2003|   6| 8.0|      20|    23.0|
            |   A|  YY|2000|   9| 3.0|      29|    26.0|
            |   A|  YY|2001|   5| 4.0|      34|    30.0|
            |   A|  YY|2002|  10| 2.0|      44|    32.0|
            |   A|  YY|2003|   5| 7.0|      49|    39.0|
            +----+----+----+----+----+--------+--------+

        * */
        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 3L,"Me_2",1.0D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 7L,"Me_2",10.0D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 14L,"Me_2",15.0D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 20L,"Me_2",23.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 29L,"Me_2",26.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 34L,"Me_2",30.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 44L,"Me_2",32.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 49L,"Me_2",39.0D)
        );

    }

    @Test
    public void testAnSumWithPartitionOrderByDPClause() throws ScriptException {

        // Analytical function count test case 4 : sum on window with partition, orderBy and data points
        /* Input dataset
        *   +----+----+----+----+----+
            |Id_1|Id_2|Year|Me_1|Me_2|
            +----+----+----+----+----+
            |   A|  XX|2000|   3| 1.0|
            |   A|  XX|2001|   4| 9.0|
            |   A|  XX|2002|   7| 5.0|
            |   A|  XX|2003|   6| 8.0|
            |   A|  YY|2000|   9| 3.0|
            |   A|  YY|2001|   5| 4.0|
            |   A|  YY|2002|  10| 2.0|
            |   A|  YY|2003|   5| 7.0|
            +----+----+----+----+----+
        * */
        ScriptContext context = engine.getContext();
        context.setAttribute("ds1", anCountDS1 , ScriptContext.ENGINE_SCOPE);


        engine.eval("res := sum ( ds1 over ( partition by Id_1 order by Id_2 data points between 2 preceding and 2 following) )");
        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);

        /*
        * The result data frame
        *
        *   +----+----+----+----+----+----------+----------+
            |Id_1|Id_2|Year|Me_1|Me_2|sum_Me_1|sum_Me_2|
            +----+----+----+----+----+--------+--------+
            |   A|  XX|2000|   3| 1.0|      14|    15.0|
            |   A|  XX|2001|   4| 9.0|      20|    23.0|
            |   A|  XX|2002|   7| 5.0|      29|    26.0|
            |   A|  XX|2003|   6| 8.0|      31|    29.0|
            |   A|  YY|2000|   9| 3.0|      37|    22.0|
            |   A|  YY|2001|   5| 4.0|      35|    24.0|
            |   A|  YY|2002|  10| 2.0|      29|    16.0|
            |   A|  YY|2003|   5| 7.0|      20|    13.0|
            +----+----+----+----+----+--------+--------+

        * */
        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 14L,"Me_2",15.0D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 20L,"Me_2",23.0D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 29L,"Me_2",26.0D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 31L,"Me_2",29.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 37L,"Me_2",22.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 35L,"Me_2",24.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 29L,"Me_2",16.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 20L,"Me_2",13.0D)
        );

    }

    @Test
    public void testAnSumWithPartitionOrderByRangeClause() throws ScriptException {

        // Analytical function count test case 5 : sum on window with partition, orderBy and range
        /* Input dataset
        *   +----+----+----+----+----+
            |Id_1|Id_2|Year|Me_1|Me_2|
            +----+----+----+----+----+
            |   A|  XX|2000|   3| 1.0|
            |   A|  XX|2001|   4| 9.0|
            |   A|  XX|2002|   7| 5.0|
            |   A|  XX|2003|   6| 8.0|
            |   A|  YY|2000|   9| 3.0|
            |   A|  YY|2001|   5| 4.0|
            |   A|  YY|2002|  10| 2.0|
            |   A|  YY|2003|   5| 7.0|
            +----+----+----+----+----+
        * */
        ScriptContext context = engine.getContext();
        context.setAttribute("ds1", anCountDS1 , ScriptContext.ENGINE_SCOPE);


        engine.eval("res := sum ( ds1 over ( partition by Id_1 order by Year range between -1 and 1) )");
        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);

        /*
        * The result data frame
        *
        *   +----+----+----+----+----+----------+----------+
            |Id_1|Id_2|Year|Me_1|Me_2|sum_Me_1|sum_Me_2|
            +----+----+----+----+----+--------+--------+
            |   A|  XX|2000|   3| 1.0|      21|    17.0|
            |   A|  YY|2000|   9| 3.0|      21|    17.0|
            |   A|  XX|2001|   4| 9.0|      38|    24.0|
            |   A|  YY|2001|   5| 4.0|      38|    24.0|
            |   A|  XX|2002|   7| 5.0|      37|    35.0|
            |   A|  YY|2002|  10| 2.0|      37|    35.0|
            |   A|  XX|2003|   6| 8.0|      28|    22.0|
            |   A|  YY|2003|   5| 7.0|      28|    22.0|
            +----+----+----+----+----+--------+--------+

        * */
        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 21L,"Me_2",17.0D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 21L,"Me_2",17.0D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 38L,"Me_2",24.0D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 38L,"Me_2",24.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 37L,"Me_2",35.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 37L,"Me_2",35.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 28L,"Me_2",22.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 28L,"Me_2",22.0D)
        );

    }

    /*
     * End of sum test case */


    /*
     * Test case for analytic function Min
     *
     * */

    @Test
    public void testAnMinWithPartitionClause() throws ScriptException {

        // Analytical function Test case 1 : min on window with partition
        /* Input dataset
        *   +----+----+----+----+----+
            |Id_1|Id_2|Year|Me_1|Me_2|
            +----+----+----+----+----+
            |   A|  XX|2000|   3| 1.0|
            |   A|  XX|2001|   4| 9.0|
            |   A|  XX|2002|   7| 5.0|
            |   A|  XX|2003|   6| 8.0|
            |   A|  YY|2000|   9| 3.0|
            |   A|  YY|2001|   5| 4.0|
            |   A|  YY|2002|  10| 2.0|
            |   A|  YY|2003|   5| 7.0|
            +----+----+----+----+----+
        * */
        ScriptContext context = engine.getContext();
        context.setAttribute("ds1", anCountDS1 , ScriptContext.ENGINE_SCOPE);


        engine.eval("res := min ( ds1 over ( partition by Id_1, Id_2 ) )");
        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);

        /*
        *
        *   +----+----+----+----+----+----------+----------+
            |Id_1|Id_2|Year|Me_1|Me_2|min_Me_1|min_Me_2|
            +----+----+----+----+----+--------+--------+
            |   A|  XX|2000|   3| 1.0|       3|     1.0|
            |   A|  XX|2001|   4| 9.0|       3|     1.0|
            |   A|  XX|2002|   7| 5.0|       3|     1.0|
            |   A|  XX|2003|   6| 8.0|       3|     1.0|
            |   A|  YY|2000|   9| 3.0|       5|     2.0|
            |   A|  YY|2001|   5| 4.0|       5|     2.0|
            |   A|  YY|2002|  10| 2.0|       5|     2.0|
            |   A|  YY|2003|   5| 7.0|       5|     2.0|
            +----+----+----+----+----+--------+--------+
        * */
        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 3L,"Me_2",1.0D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 3L,"Me_2",1.0D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 3L,"Me_2",1.0D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 3L,"Me_2",1.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 5L,"Me_2",2.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 5L,"Me_2",2.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 5L,"Me_2",2.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 5L,"Me_2",2.0D)
        );

    }

    @Test
    public void testAnMinWithPartitionOrderByClause() throws ScriptException {

        // Analytical function Test case 2 : min on window with partition and order by
        /* Input dataset
        *   +----+----+----+----+----+
            |Id_1|Id_2|Year|Me_1|Me_2|
            +----+----+----+----+----+
            |   A|  XX|2000|   3| 1.0|
            |   A|  XX|2001|   4| 9.0|
            |   A|  XX|2002|   7| 5.0|
            |   A|  XX|2003|   6| 8.0|
            |   A|  YY|2000|   9| 3.0|
            |   A|  YY|2001|   5| 4.0|
            |   A|  YY|2002|  10| 2.0|
            |   A|  YY|2003|   5| 7.0|
            +----+----+----+----+----+
        * */
        ScriptContext context = engine.getContext();
        context.setAttribute("ds1", anCountDS1 , ScriptContext.ENGINE_SCOPE);


        engine.eval("res := min ( ds1 over ( partition by Id_1, Id_2 order by Year) )");
        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);

        /*
        *   +----+----+----+----+----+----------+----------+
            |Id_1|Id_2|Year|Me_1|Me_2|min_Me_1|min_Me_2|
            +----+----+----+----+----+--------+--------+
            |   A|  XX|2000|   3| 1.0|       3|     1.0|
            |   A|  XX|2001|   4| 9.0|       3|     1.0|
            |   A|  XX|2002|   7| 5.0|       3|     1.0|
            |   A|  XX|2003|   6| 8.0|       3|     1.0|
            |   A|  YY|2000|   9| 3.0|       9|     3.0|
            |   A|  YY|2001|   5| 4.0|       5|     3.0|
            |   A|  YY|2002|  10| 2.0|       5|     2.0|
            |   A|  YY|2003|   5| 7.0|       5|     2.0|
            +----+----+----+----+----+--------+--------+

        * */
        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 3L,"Me_2",1.0D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 3L,"Me_2",1.0D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 3L,"Me_2",1.0D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 3L,"Me_2",1.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 9L,"Me_2",3.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 5L,"Me_2",3.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 5L,"Me_2",2.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 5L,"Me_2",2.0D)
        );

    }

    @Test
    public void testAnMinWithOrderByClause() throws ScriptException {

        // Analytical function Test case 3 : min on window with only order by without partition
        /* Input dataset
        *   +----+----+----+----+----+
            |Id_1|Id_2|Year|Me_1|Me_2|
            +----+----+----+----+----+
            |   A|  XX|2000|   3| 1.0|
            |   A|  XX|2001|   4| 9.0|
            |   A|  XX|2002|   7| 5.0|
            |   A|  XX|2003|   6| 8.0|
            |   A|  YY|2000|   9| 3.0|
            |   A|  YY|2001|   5| 4.0|
            |   A|  YY|2002|  10| 2.0|
            |   A|  YY|2003|   5| 7.0|
            +----+----+----+----+----+
        * */
        ScriptContext context = engine.getContext();
        context.setAttribute("ds1", anCountDS1 , ScriptContext.ENGINE_SCOPE);


        engine.eval("res := min ( ds1 over ( order by Id_1, Id_2, Year ) )");
        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);

        /*
        *
        *   +----+----+----+----+----+----------+----------+
            |Id_1|Id_2|Year|Me_1|Me_2|min_Me_1|min_Me_2|
            +----+----+----+----+----+--------+--------+
            |   A|  XX|2000|   3| 1.0|       3|     1.0|
            |   A|  XX|2001|   4| 9.0|       3|     1.0|
            |   A|  XX|2002|   7| 5.0|       3|     1.0|
            |   A|  XX|2003|   6| 8.0|       3|     1.0|
            |   A|  YY|2000|   9| 3.0|       3|     1.0|
            |   A|  YY|2001|   5| 4.0|       3|     1.0|
            |   A|  YY|2002|  10| 2.0|       3|     1.0|
            |   A|  YY|2003|   5| 7.0|       3|     1.0|
            +----+----+----+----+----+--------+--------+

        * */
        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 3L,"Me_2",1.0D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 3L,"Me_2",1.0D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 3L,"Me_2",1.0D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 3L,"Me_2",1.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 3L,"Me_2",1.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 3L,"Me_2",1.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 3L,"Me_2",1.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 3L,"Me_2",1.0D)
        );

    }

    @Test
    public void testAnMinWithPartitionOrderByDPClause() throws ScriptException {

        // Analytical function count test case 4 : min on window with partition, orderBy and data points
        /* Input dataset
        *   +----+----+----+----+----+
            |Id_1|Id_2|Year|Me_1|Me_2|
            +----+----+----+----+----+
            |   A|  XX|2000|   3| 1.0|
            |   A|  XX|2001|   4| 9.0|
            |   A|  XX|2002|   7| 5.0|
            |   A|  XX|2003|   6| 8.0|
            |   A|  YY|2000|   9| 3.0|
            |   A|  YY|2001|   5| 4.0|
            |   A|  YY|2002|  10| 2.0|
            |   A|  YY|2003|   5| 7.0|
            +----+----+----+----+----+
        * */
        ScriptContext context = engine.getContext();
        context.setAttribute("ds1", anCountDS1 , ScriptContext.ENGINE_SCOPE);

        engine.eval("res := min ( ds1 over ( partition by Id_1 order by Id_2 data points between 2 preceding and 2 following) )");
        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);

        /*
        * The result data frame
        *
        *   +----+----+----+----+----+----------+----------+
            |Id_1|Id_2|Year|Me_1|Me_2|min_Me_1|min_Me_2|
            +----+----+----+----+----+--------+--------+
            |   A|  XX|2000|   3| 1.0|       3|     1.0|
            |   A|  XX|2001|   4| 9.0|       3|     1.0|
            |   A|  XX|2002|   7| 5.0|       3|     1.0|
            |   A|  XX|2003|   6| 8.0|       4|     3.0|
            |   A|  YY|2000|   9| 3.0|       5|     2.0|
            |   A|  YY|2001|   5| 4.0|       5|     2.0|
            |   A|  YY|2002|  10| 2.0|       5|     2.0|
            |   A|  YY|2003|   5| 7.0|       5|     2.0|
            +----+----+----+----+----+--------+--------+

        * */
        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 3L,"Me_2",1.0D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 3L,"Me_2",1.0D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 3L,"Me_2",1.0D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 4L,"Me_2",3.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 5L,"Me_2",2.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 5L,"Me_2",2.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 5L,"Me_2",2.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 5L,"Me_2",2.0D)
        );

    }

    @Test
    public void testAnMinWithPartitionOrderByRangeClause() throws ScriptException {

        // Analytical function count test case 5 : min on window with partition, orderBy and range
        /* Input dataset
        *   +----+----+----+----+----+
            |Id_1|Id_2|Year|Me_1|Me_2|
            +----+----+----+----+----+
            |   A|  XX|2000|   3| 1.0|
            |   A|  XX|2001|   4| 9.0|
            |   A|  XX|2002|   7| 5.0|
            |   A|  XX|2003|   6| 8.0|
            |   A|  YY|2000|   9| 3.0|
            |   A|  YY|2001|   5| 4.0|
            |   A|  YY|2002|  10| 2.0|
            |   A|  YY|2003|   5| 7.0|
            +----+----+----+----+----+
        * */
        ScriptContext context = engine.getContext();
        context.setAttribute("ds1", anCountDS1 , ScriptContext.ENGINE_SCOPE);


        engine.eval("res := min ( ds1 over ( partition by Id_1 order by Year range between -1 and 1) )");
        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);

        /*
        * The result data frame
        *
        *   +----+----+----+----+----+----------+----------+
            |Id_1|Id_2|Year|Me_1|Me_2|min_Me_1|min_Me_2|
            +----+----+----+----+----+--------+--------+
            |   A|  XX|2000|   3| 1.0|       3|     1.0|
            |   A|  YY|2000|   9| 3.0|       3|     1.0|
            |   A|  XX|2001|   4| 9.0|       3|     1.0|
            |   A|  YY|2001|   5| 4.0|       3|     1.0|
            |   A|  XX|2002|   7| 5.0|       4|     2.0|
            |   A|  YY|2002|  10| 2.0|       4|     2.0|
            |   A|  XX|2003|   6| 8.0|       5|     2.0|
            |   A|  YY|2003|   5| 7.0|       5|     2.0|
            +----+----+----+----+----+--------+--------+

        * */
        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 3L,"Me_2",1.0D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 3L,"Me_2",1.0D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 3L,"Me_2",1.0D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 3L,"Me_2",1.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 4L,"Me_2",2.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 4L,"Me_2",2.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 5L,"Me_2",2.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 5L,"Me_2",2.0D)
        );

    }

    /*
     * End of Min test case */


    /*
     * Test case for analytic function Max
     *
     * */
    @Test
    public void testAnMaxWithPartitionClause() throws ScriptException {

        // Analytical function Test case 1 : max on window with partition
        /* Input dataset
        *   +----+----+----+----+----+
            |Id_1|Id_2|Year|Me_1|Me_2|
            +----+----+----+----+----+
            |   A|  XX|2000|   3| 1.0|
            |   A|  XX|2001|   4| 9.0|
            |   A|  XX|2002|   7| 5.0|
            |   A|  XX|2003|   6| 8.0|
            |   A|  YY|2000|   9| 3.0|
            |   A|  YY|2001|   5| 4.0|
            |   A|  YY|2002|  10| 2.0|
            |   A|  YY|2003|   5| 7.0|
            +----+----+----+----+----+
        * */
        ScriptContext context = engine.getContext();
        context.setAttribute("ds1", anCountDS1 , ScriptContext.ENGINE_SCOPE);


        engine.eval("res := max ( ds1 over ( partition by Id_1, Id_2 ) )");
        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);

        /*
        *
        *   +----+----+----+----+----+----------+----------+
            |Id_1|Id_2|Year|Me_1|Me_2|max_Me_1|max_Me_2|
            +----+----+----+----+----+--------+--------+
            |   A|  XX|2000|   3| 1.0|       7|     9.0|
            |   A|  XX|2001|   4| 9.0|       7|     9.0|
            |   A|  XX|2002|   7| 5.0|       7|     9.0|
            |   A|  XX|2003|   6| 8.0|       7|     9.0|
            |   A|  YY|2000|   9| 3.0|      10|     7.0|
            |   A|  YY|2001|   5| 4.0|      10|     7.0|
            |   A|  YY|2002|  10| 2.0|      10|     7.0|
            |   A|  YY|2003|   5| 7.0|      10|     7.0|
            +----+----+----+----+----+--------+--------+
        * */
        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 7L,"Me_2",9.0D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 7L,"Me_2",9.0D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 7L,"Me_2",9.0D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 7L,"Me_2",9.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 10L,"Me_2",7.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 10L,"Me_2",7.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 10L,"Me_2",7.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 10L,"Me_2",7.0D)
        );

    }

    @Test
    public void testAnMaxWithPartitionOrderByClause() throws ScriptException {

        // Analytical function Test case 2 : max on window with partition and order by
        /* Input dataset
        *   +----+----+----+----+----+
            |Id_1|Id_2|Year|Me_1|Me_2|
            +----+----+----+----+----+
            |   A|  XX|2000|   3| 1.0|
            |   A|  XX|2001|   4| 9.0|
            |   A|  XX|2002|   7| 5.0|
            |   A|  XX|2003|   6| 8.0|
            |   A|  YY|2000|   9| 3.0|
            |   A|  YY|2001|   5| 4.0|
            |   A|  YY|2002|  10| 2.0|
            |   A|  YY|2003|   5| 7.0|
            +----+----+----+----+----+
        * */
        ScriptContext context = engine.getContext();
        context.setAttribute("ds1", anCountDS1 , ScriptContext.ENGINE_SCOPE);


        engine.eval("res := max ( ds1 over ( partition by Id_1, Id_2 order by Year) )");
        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);

        /*
        *   +----+----+----+----+----+----------+----------+
            |Id_1|Id_2|Year|Me_1|Me_2|max_Me_1|max_Me_2|
            +----+----+----+----+----+--------+--------+
            |   A|  XX|2000|   3| 1.0|       3|     1.0|
            |   A|  XX|2001|   4| 9.0|       4|     9.0|
            |   A|  XX|2002|   7| 5.0|       7|     9.0|
            |   A|  XX|2003|   6| 8.0|       7|     9.0|
            |   A|  YY|2000|   9| 3.0|       9|     3.0|
            |   A|  YY|2001|   5| 4.0|       9|     4.0|
            |   A|  YY|2002|  10| 2.0|      10|     4.0|
            |   A|  YY|2003|   5| 7.0|      10|     7.0|
            +----+----+----+----+----+--------+--------+

        * */
        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 3L,"Me_2",1.0D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 4L,"Me_2",9.0D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 7L,"Me_2",9.0D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 7L,"Me_2",9.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 9L,"Me_2",3.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 9L,"Me_2",4.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 10L,"Me_2",4.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 10L,"Me_2",7.0D)
        );

    }


    @Test
    public void testAnMaxWithPartitionOrderByDPClause() throws ScriptException {

        // Analytical function count test case 3 : max on window with partition, orderBy and data points
        /* Input dataset
        *   +----+----+----+----+----+
            |Id_1|Id_2|Year|Me_1|Me_2|
            +----+----+----+----+----+
            |   A|  XX|2000|   3| 1.0|
            |   A|  XX|2001|   4| 9.0|
            |   A|  XX|2002|   7| 5.0|
            |   A|  XX|2003|   6| 8.0|
            |   A|  YY|2000|   9| 3.0|
            |   A|  YY|2001|   5| 4.0|
            |   A|  YY|2002|  10| 2.0|
            |   A|  YY|2003|   5| 7.0|
            +----+----+----+----+----+
        * */
        ScriptContext context = engine.getContext();
        context.setAttribute("ds1", anCountDS1 , ScriptContext.ENGINE_SCOPE);

        engine.eval("res := max ( ds1 over ( partition by Id_1 order by Id_2 data points between 2 preceding and 2 following) )");
        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);

        /*
        * The result data frame
        *
        *   +----+----+----+----+----+----------+----------+
            |Id_1|Id_2|Year|Me_1|Me_2|max_Me_1|max_Me_2|
            +----+----+----+----+----+--------+--------+
            |   A|  XX|2000|   3| 1.0|       7|     9.0|
            |   A|  XX|2001|   4| 9.0|       7|     9.0|
            |   A|  XX|2002|   7| 5.0|       9|     9.0|
            |   A|  XX|2003|   6| 8.0|       9|     9.0|
            |   A|  YY|2000|   9| 3.0|      10|     8.0|
            |   A|  YY|2001|   5| 4.0|      10|     8.0|
            |   A|  YY|2002|  10| 2.0|      10|     7.0|
            |   A|  YY|2003|   5| 7.0|      10|     7.0|
            +----+----+----+----+----+--------+--------+

        * */
        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 7L,"Me_2",9.0D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 7L,"Me_2",9.0D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 9L,"Me_2",9.0D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 9L,"Me_2",9.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 10L,"Me_2",8.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 10L,"Me_2",8.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 10L,"Me_2",7.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 10L,"Me_2",7.0D)
        );

    }

    @Test
    public void testAnMaxWithPartitionOrderByRangeClause() throws ScriptException {

        // Analytical function count test case 5 : min on window with partition, orderBy and range
        /* Input dataset
        *   +----+----+----+----+----+
            |Id_1|Id_2|Year|Me_1|Me_2|
            +----+----+----+----+----+
            |   A|  XX|2000|   3| 1.0|
            |   A|  XX|2001|   4| 9.0|
            |   A|  XX|2002|   7| 5.0|
            |   A|  XX|2003|   6| 8.0|
            |   A|  YY|2000|   9| 3.0|
            |   A|  YY|2001|   5| 4.0|
            |   A|  YY|2002|  10| 2.0|
            |   A|  YY|2003|   5| 7.0|
            +----+----+----+----+----+
        * */
        ScriptContext context = engine.getContext();
        context.setAttribute("ds1", anCountDS1 , ScriptContext.ENGINE_SCOPE);


        engine.eval("res := min ( ds1 over ( partition by Id_1 order by Year range between -1 and 1) )");
        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);

        /*
        * The result data frame
        *
        *   +----+----+----+----+----+----------+----------+
            |Id_1|Id_2|Year|Me_1|Me_2|max_Me_1|max_Me_2|
            +----+----+----+----+----+--------+--------+
            |   A|  XX|2000|   3| 1.0|       9|     9.0|
            |   A|  YY|2000|   9| 3.0|       9|     9.0|
            |   A|  XX|2001|   4| 9.0|      10|     9.0|
            |   A|  YY|2001|   5| 4.0|      10|     9.0|
            |   A|  XX|2002|   7| 5.0|      10|     9.0|
            |   A|  YY|2002|  10| 2.0|      10|     9.0|
            |   A|  XX|2003|   6| 8.0|      10|     8.0|
            |   A|  YY|2003|   5| 7.0|      10|     8.0|
            +----+----+----+----+----+--------+--------+

        * */
        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 9L,"Me_2",9.0D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 9L,"Me_2",9.0D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 10L,"Me_2",9.0D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 10L,"Me_2",9.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 10L,"Me_2",9.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 10L,"Me_2",9.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 10L,"Me_2",8.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 10L,"Me_2",8.0D)
        );

    }
    /*
     * End of Max test case */

    /*
     * Test case for analytic function Avg
     *
     * */
    @Test
    public void testAnAvgWithPartitionClause() throws ScriptException {

        // Analytical function Test case 1 : avg on window with partition
        /* Input dataset
        *   +----+----+----+----+----+
            |Id_1|Id_2|Year|Me_1|Me_2|
            +----+----+----+----+----+
            |   A|  XX|2000|   3| 1.0|
            |   A|  XX|2001|   4| 9.0|
            |   A|  XX|2002|   7| 5.0|
            |   A|  XX|2003|   6| 8.0|
            |   A|  YY|2000|   9| 3.0|
            |   A|  YY|2001|   5| 4.0|
            |   A|  YY|2002|  10| 2.0|
            |   A|  YY|2003|   5| 7.0|
            +----+----+----+----+----+
        * */
        ScriptContext context = engine.getContext();
        context.setAttribute("ds1", anCountDS1 , ScriptContext.ENGINE_SCOPE);


        engine.eval("res := avg ( ds1 over ( partition by Id_1, Id_2 ) )");
        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);

        /*
        *
        *   +----+----+----+----+----+----------+----------+
            |Id_1|Id_2|Year|Me_1|Me_2|avg_Me_1|avg_Me_2|
            +----+----+----+----+----+--------+--------+
            |   A|  XX|2000|   3| 1.0|     5.0|    5.75|
            |   A|  XX|2001|   4| 9.0|     5.0|    5.75|
            |   A|  XX|2002|   7| 5.0|     5.0|    5.75|
            |   A|  XX|2003|   6| 8.0|     5.0|    5.75|
            |   A|  YY|2000|   9| 3.0|    7.25|     4.0|
            |   A|  YY|2001|   5| 4.0|    7.25|     4.0|
            |   A|  YY|2002|  10| 2.0|    7.25|     4.0|
            |   A|  YY|2003|   5| 7.0|    7.25|     4.0|
            +----+----+----+----+----+--------+--------+
        * */
        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 5.0D,"Me_2",5.75D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 5.0D,"Me_2",5.75D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 5.0D,"Me_2",5.75D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 5.0D,"Me_2",5.75D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 7.25D,"Me_2",4.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 7.25D,"Me_2",4.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 7.25D,"Me_2",4.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 7.25D,"Me_2",4.0D)
        );

    }

    @Test
    public void testAnAvgWithPartitionOrderByClause() throws ScriptException {

        // Analytical function Test case 2 : avg on window with partition and order by
        /* Input dataset
        *   +----+----+----+----+----+
            |Id_1|Id_2|Year|Me_1|Me_2|
            +----+----+----+----+----+
            |   A|  XX|2000|   3| 1.0|
            |   A|  XX|2001|   4| 9.0|
            |   A|  XX|2002|   7| 5.0|
            |   A|  XX|2003|   6| 8.0|
            |   A|  YY|2000|   9| 3.0|
            |   A|  YY|2001|   5| 4.0|
            |   A|  YY|2002|  10| 2.0|
            |   A|  YY|2003|   5| 7.0|
            +----+----+----+----+----+
        * */
        ScriptContext context = engine.getContext();
        context.setAttribute("ds1", anCountDS1 , ScriptContext.ENGINE_SCOPE);


        engine.eval("res := avg ( ds1 over ( partition by Id_1, Id_2 order by Year) )");
        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);

        /*
        *   +----+----+----+----+----+----------+----------+
            |Id_1|Id_2|Year|Me_1|Me_2|         avg_Me_1|avg_Me_2|
            +----+----+----+----+----+-----------------+--------+
            |   A|  XX|2000|   3| 1.0|              3.0|     1.0|
            |   A|  XX|2001|   4| 9.0|              3.5|     5.0|
            |   A|  XX|2002|   7| 5.0|4.666666666666667|     5.0|
            |   A|  XX|2003|   6| 8.0|              5.0|    5.75|
            |   A|  YY|2000|   9| 3.0|              9.0|     3.0|
            |   A|  YY|2001|   5| 4.0|              7.0|     3.5|
            |   A|  YY|2002|  10| 2.0|              8.0|     3.0|
            |   A|  YY|2003|   5| 7.0|             7.25|     4.0|
            +----+----+----+----+----+-----------------+--------+

        * */
        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 3.0D,"Me_2",1.0D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 3.5D,"Me_2",5.0D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 4.67D,"Me_2",5.0D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 5.0D,"Me_2",5.75D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 9.0D,"Me_2",3.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 7.0D,"Me_2",3.5D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 8.0D,"Me_2",3.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 7.25D,"Me_2",4.0D)
        );

    }


    @Test
    public void testAnAvgWithPartitionOrderByDPClause() throws ScriptException {

        // Analytical function count test case 3 : avg on window with partition, orderBy and data points
        /* Input dataset
        *   +----+----+----+----+----+
            |Id_1|Id_2|Year|Me_1|Me_2|
            +----+----+----+----+----+
            |   A|  XX|2000|   3| 1.0|
            |   A|  XX|2001|   4| 9.0|
            |   A|  XX|2002|   7| 5.0|
            |   A|  XX|2003|   6| 8.0|
            |   A|  YY|2000|   9| 3.0|
            |   A|  YY|2001|   5| 4.0|
            |   A|  YY|2002|  10| 2.0|
            |   A|  YY|2003|   5| 7.0|
            +----+----+----+----+----+
        * */
        ScriptContext context = engine.getContext();
        context.setAttribute("ds1", anCountDS1 , ScriptContext.ENGINE_SCOPE);

        engine.eval("res := avg ( ds1 over ( partition by Id_1 order by Id_2 data points between 2 preceding and 2 following) )");
        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);

        /*
        * The result data frame
        *
        *   +----+----+----+----+----+----------+----------+
            |Id_1|Id_2|Year|Me_1|Me_2|         avg_Me_1|         avg_Me_2|
            +----+----+----+----+----+-----------------+-----------------+
            |   A|  XX|2000|   3| 1.0|4.666666666666667|              5.0|
            |   A|  XX|2001|   4| 9.0|              5.0|             5.75|
            |   A|  XX|2002|   7| 5.0|              5.8|              5.2|
            |   A|  XX|2003|   6| 8.0|              6.2|              5.8|
            |   A|  YY|2000|   9| 3.0|              7.4|              4.4|
            |   A|  YY|2001|   5| 4.0|              7.0|              4.8|
            |   A|  YY|2002|  10| 2.0|             7.25|              4.0|
            |   A|  YY|2003|   5| 7.0|6.666666666666667|4.333333333333333|
            +----+----+----+----+----+-----------------+-----------------+

        * */
        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 4.67D,"Me_2",5.0D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 5.0D,"Me_2",5.75D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 5.8D,"Me_2",5.2D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 6.2D,"Me_2",5.8D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 7.4D,"Me_2",4.4D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 7.0D,"Me_2",4.8D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 7.25D,"Me_2",4.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 6.67D,"Me_2",4.33D)
        );

    }

    @Test
    public void testAnAvgWithPartitionOrderByRangeClause() throws ScriptException {

        // Analytical function count test case 5 : avg on window with partition, orderBy and range
        /* Input dataset
        *   +----+----+----+----+----+
            |Id_1|Id_2|Year|Me_1|Me_2|
            +----+----+----+----+----+
            |   A|  XX|2000|   3| 1.0|
            |   A|  XX|2001|   4| 9.0|
            |   A|  XX|2002|   7| 5.0|
            |   A|  XX|2003|   6| 8.0|
            |   A|  YY|2000|   9| 3.0|
            |   A|  YY|2001|   5| 4.0|
            |   A|  YY|2002|  10| 2.0|
            |   A|  YY|2003|   5| 7.0|
            +----+----+----+----+----+
        * */
        ScriptContext context = engine.getContext();
        context.setAttribute("ds1", anCountDS1 , ScriptContext.ENGINE_SCOPE);


        engine.eval("res := avg ( ds1 over ( partition by Id_1 order by Year range between -1 and 1) )");
        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);

        /*
        * The result data frame
        *
        *   +----+----+----+----+----+----------+----------+
            |Id_1|Id_2|Year|Me_1|Me_2|         avg_Me_1|         avg_Me_2|
            +----+----+----+----+----+-----------------+-----------------+
            |   A|  XX|2000|   3| 1.0|             5.25|             4.25|
            |   A|  YY|2000|   9| 3.0|             5.25|             4.25|
            |   A|  XX|2001|   4| 9.0|6.333333333333333|              4.0|
            |   A|  YY|2001|   5| 4.0|6.333333333333333|              4.0|
            |   A|  XX|2002|   7| 5.0|6.166666666666667|5.833333333333333|
            |   A|  YY|2002|  10| 2.0|6.166666666666667|5.833333333333333|
            |   A|  XX|2003|   6| 8.0|              7.0|              5.5|
            |   A|  YY|2003|   5| 7.0|              7.0|              5.5|
            +----+----+----+----+----+-----------------+-----------------+

        * */
        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 5.25D,"Me_2",4.25D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 5.25D,"Me_2",4.25D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 6.33D,"Me_2",4.0D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 6.33D,"Me_2",4.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 6.17D,"Me_2",5.83D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 6.17D,"Me_2",5.83D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 7.0D,"Me_2",5.5D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 7.0D,"Me_2",5.5D)
        );

    }
    /*
     * End of Avg test case */

    /*
     * Test case for analytic function Median
     *
     * */

    /*
     * End of Median test case */

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