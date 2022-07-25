package fr.insee.vtl.spark;

import fr.insee.vtl.engine.VtlScriptEngine;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.Dataset.Role;
import fr.insee.vtl.model.InMemoryDataset;
import fr.insee.vtl.model.ProcessingEngineFactory;
import org.apache.spark.sql.SparkSession;
import org.assertj.core.api.ListAssert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.util.*;
import java.util.stream.Collectors;

import static fr.insee.vtl.model.Structured.Component;
import static org.assertj.core.api.Assertions.assertThat;

public class SparkProcessingEngineTest {

    private SparkSession spark;
    private ScriptEngine engine;

    private final InMemoryDataset anCountDS1 = new InMemoryDataset(
            List.of(
                    Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 3L, "Me_2", 1D),
                    Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 4L, "Me_2", 9D),
                    Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 7L, "Me_2", 5D),
                    Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 6L, "Me_2", 8D),
                    Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 9L, "Me_2", 3D),
                    Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 5L, "Me_2", 4D),
                    Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 10L, "Me_2", 2D),
                    Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 5L, "Me_2", 7D)

            ),
            Map.of("Id_1", String.class, "Id_2", String.class, "Year", Long.class, "Me_1", Long.class, "Me_2", Double.class),
            Map.of("Id_1", Role.IDENTIFIER, "Id_2", Role.IDENTIFIER, "Year", Role.IDENTIFIER, "Me_1", Role.MEASURE, "Me_2", Role.MEASURE)
    );

    private final InMemoryDataset anCountDS2 = new InMemoryDataset(
            List.of(
                    Map.of("Id_1", "A", "Id_2", "XX", "Year", 1993L, "Me_1", 3L, "Me_2", 1D),
                    Map.of("Id_1", "A", "Id_2", "XX", "Year", 1994L, "Me_1", 4L, "Me_2", 9D),
                    Map.of("Id_1", "A", "Id_2", "XX", "Year", 1995L, "Me_1", 7L, "Me_2", 5D),
                    Map.of("Id_1", "A", "Id_2", "XX", "Year", 1996L, "Me_1", 6L, "Me_2", 8D),
                    Map.of("Id_1", "A", "Id_2", "YY", "Year", 1993L, "Me_1", 9L, "Me_2", 3D),
                    Map.of("Id_1", "A", "Id_2", "YY", "Year", 1994L, "Me_1", 5L, "Me_2", 4D),
                    Map.of("Id_1", "A", "Id_2", "YY", "Year", 1995L, "Me_1", 10L, "Me_2", 2D),
                    Map.of("Id_1", "A", "Id_2", "YY", "Year", 1996L, "Me_1", 2L, "Me_2", 7D)

            ),
            Map.of("Id_1", String.class, "Id_2", String.class, "Year", Long.class, "Me_1", Long.class, "Me_2", Double.class),
            Map.of("Id_1", Role.IDENTIFIER, "Id_2", Role.IDENTIFIER, "Year", Role.IDENTIFIER, "Me_1", Role.MEASURE, "Me_2", Role.MEASURE)
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
    private final String DEFAULT_NULL_STR="null";
    private static <T, K> Map<K, T> replaceNullValues(Map<K, T> map, T defaultValue)
    {

        // Replace the null value
        map = map.entrySet()
                .stream()
                .map(entry -> {
                    if (entry.getValue() == null)
                        entry.setValue(defaultValue);
                    return entry;
                })
                .collect(Collectors.toMap(Map.Entry::getKey,
                        Map.Entry::getValue));

        return map;
    }

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
     * Test with calc statment */
    @Test
    public void testAnCountWithCalcClause() throws ScriptException {

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
        context.setAttribute("ds1", anCountDS1, ScriptContext.ENGINE_SCOPE);


        engine.eval("res := ds1 [ calc count_Me_1:= count ( Me_1 over ( partition by Id_1,Id_2 order by Year ) )];");
        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);

        /*
        *
        *   +----+----+----+----+----+----------+
            |Id_1|Id_2|Year|Me_1|Me_2|count_Me_1|
            +----+----+----+----+----+----------+
            |   A|  XX|2000|   3| 1.0|         1|
            |   A|  XX|2001|   4| 9.0|         2|
            |   A|  XX|2002|   7| 5.0|         3|
            |   A|  XX|2003|   6| 8.0|         4|
            |   A|  YY|2000|   9| 3.0|         1|
            |   A|  YY|2001|   5| 4.0|         2|
            |   A|  YY|2002|  10| 2.0|         3|
            |   A|  YY|2003|   5| 7.0|         4|
            +----+----+----+----+----+----------+
        * */
        List<Map<String, Object>> actual = ((Dataset) engine.getContext().getAttribute("res")).getDataAsMap();

        assertThat(actual).containsExactly(
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 3L, "Me_2", 1.0D, "count_Me_1", 1L),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 4L, "Me_2", 9.0D, "count_Me_1", 2L),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 7L, "Me_2", 5.0D, "count_Me_1", 3L),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 6L, "Me_2", 8.0D, "count_Me_1", 4L),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 9L, "Me_2", 3.0D, "count_Me_1", 1L),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 5L, "Me_2", 4.0D, "count_Me_1", 2L),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 10L, "Me_2", 2.0D, "count_Me_1", 3L),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 5L, "Me_2", 7.0D, "count_Me_1", 4L)
        );

    }

    @Test
    public void testAnCountDPWithCalcClause() throws ScriptException {

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
        context.setAttribute("ds1", anCountDS1, ScriptContext.ENGINE_SCOPE);


        engine.eval("res := ds1 [ calc count_Me_1:= count ( Me_1 over ( partition by Id_1,Id_2 order by Year data points between 2 preceding and 2 following) )];");
        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);

        /*
        *
        +----+----+----+----+----+----------+
        |Id_1|Id_2|Year|Me_1|Me_2|count_Me_1|
        +----+----+----+----+----+----------+
        |   A|  XX|2000|   3| 1.0|         3|
        |   A|  XX|2001|   4| 9.0|         4|
        |   A|  XX|2002|   7| 5.0|         4|
        |   A|  XX|2003|   6| 8.0|         3|
        |   A|  YY|2000|   9| 3.0|         3|
        |   A|  YY|2001|   5| 4.0|         4|
        |   A|  YY|2002|  10| 2.0|         4|
        |   A|  YY|2003|   5| 7.0|         3|
        +----+----+----+----+----+----------+
        * */
        List<Map<String, Object>> actual = ((Dataset) engine.getContext().getAttribute("res")).getDataAsMap();

        assertThat(actual).containsExactly(
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 3L, "Me_2", 1.0D, "count_Me_1", 3L),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 4L, "Me_2", 9.0D, "count_Me_1", 4L),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 7L, "Me_2", 5.0D, "count_Me_1", 4L),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 6L, "Me_2", 8.0D, "count_Me_1", 3L),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 9L, "Me_2", 3.0D, "count_Me_1", 3L),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 5L, "Me_2", 4.0D, "count_Me_1", 4L),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 10L, "Me_2", 2.0D, "count_Me_1", 4L),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 5L, "Me_2", 7.0D, "count_Me_1", 3L)
        );

    }

    @Test
    public void testAnCountRangeWithCalcClause() throws ScriptException {

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
        context.setAttribute("ds1", anCountDS1, ScriptContext.ENGINE_SCOPE);


        engine.eval("res := ds1 [ calc count_Me_1:= count ( Me_1 over ( partition by Id_1,Id_2 order by Year range between 1 preceding and 1 following) )];");
        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);

        /*
        *
        +----+----+----+----+----+----------+
        |Id_1|Id_2|Year|Me_1|Me_2|count_Me_1|
        +----+----+----+----+----+----------+
        |   A|  XX|2000|   3| 1.0|         2|
        |   A|  XX|2001|   4| 9.0|         3|
        |   A|  XX|2002|   7| 5.0|         3|
        |   A|  XX|2003|   6| 8.0|         2|
        |   A|  YY|2000|   9| 3.0|         2|
        |   A|  YY|2001|   5| 4.0|         3|
        |   A|  YY|2002|  10| 2.0|         3|
        |   A|  YY|2003|   5| 7.0|         2|
        +----+----+----+----+----+----------+
        * */
        List<Map<String, Object>> actual = ((Dataset) engine.getContext().getAttribute("res")).getDataAsMap();

        assertThat(actual).containsExactly(
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 3L, "Me_2", 1.0D, "count_Me_1", 2L),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 4L, "Me_2", 9.0D, "count_Me_1", 3L),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 7L, "Me_2", 5.0D, "count_Me_1", 3L),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 6L, "Me_2", 8.0D, "count_Me_1", 2L),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 9L, "Me_2", 3.0D, "count_Me_1", 2L),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 5L, "Me_2", 4.0D, "count_Me_1", 3L),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 10L, "Me_2", 2.0D, "count_Me_1", 3L),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 5L, "Me_2", 7.0D, "count_Me_1", 2L)
        );

    }

    @Test
    public void testAnSumWithCalcClause() throws ScriptException {

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
        context.setAttribute("ds1", anCountDS1, ScriptContext.ENGINE_SCOPE);


        engine.eval("res := ds1 [ calc sum_Me_1:= sum ( Me_1 over ( partition by Id_1,Id_2 order by Year) )];");
        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);

        /*
        *
        +----+----+----+----+----+--------+
        |Id_1|Id_2|Year|Me_1|Me_2|sum_Me_1|
        +----+----+----+----+----+--------+
        |   A|  XX|2000|   3| 1.0|       3|
        |   A|  XX|2001|   4| 9.0|       7|
        |   A|  XX|2002|   7| 5.0|      14|
        |   A|  XX|2003|   6| 8.0|      20|
        |   A|  YY|2000|   9| 3.0|       9|
        |   A|  YY|2001|   5| 4.0|      14|
        |   A|  YY|2002|  10| 2.0|      24|
        |   A|  YY|2003|   5| 7.0|      29|
        +----+----+----+----+----+--------+
        * */
        List<Map<String, Object>> actual = ((Dataset) engine.getContext().getAttribute("res")).getDataAsMap();

        assertThat(actual).containsExactly(
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 3L, "Me_2", 1.0D, "sum_Me_1", 3L),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 4L, "Me_2", 9.0D, "sum_Me_1", 7L),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 7L, "Me_2", 5.0D, "sum_Me_1", 14L),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 6L, "Me_2", 8.0D, "sum_Me_1", 20L),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 9L, "Me_2", 3.0D, "sum_Me_1", 9L),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 5L, "Me_2", 4.0D, "sum_Me_1", 14L),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 10L, "Me_2", 2.0D, "sum_Me_1", 24L),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 5L, "Me_2", 7.0D, "sum_Me_1", 29L)
        );

    }

    @Test
    public void testAnMinWithCalcClause() throws ScriptException {

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
        context.setAttribute("ds1", anCountDS1, ScriptContext.ENGINE_SCOPE);


        engine.eval("res := ds1 [ calc min_Me_1:= min ( Me_1 over ( partition by Id_1,Id_2 order by Year) )];");
        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);

        /*
        *
        +----+----+----+----+----+--------+
        |Id_1|Id_2|Year|Me_1|Me_2|min_Me_1|
        +----+----+----+----+----+--------+
        |   A|  XX|2000|   3| 1.0|       3|
        |   A|  XX|2001|   4| 9.0|       3|
        |   A|  XX|2002|   7| 5.0|       3|
        |   A|  XX|2003|   6| 8.0|       3|
        |   A|  YY|2000|   9| 3.0|       9|
        |   A|  YY|2001|   5| 4.0|       5|
        |   A|  YY|2002|  10| 2.0|       5|
        |   A|  YY|2003|   5| 7.0|       5|
        +----+----+----+----+----+--------+
        * */
        List<Map<String, Object>> actual = ((Dataset) engine.getContext().getAttribute("res")).getDataAsMap();

        assertThat(actual).containsExactly(
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 3L, "Me_2", 1.0D, "min_Me_1", 3L),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 4L, "Me_2", 9.0D, "min_Me_1", 3L),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 7L, "Me_2", 5.0D, "min_Me_1", 3L),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 6L, "Me_2", 8.0D, "min_Me_1", 3L),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 9L, "Me_2", 3.0D, "min_Me_1", 9L),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 5L, "Me_2", 4.0D, "min_Me_1", 5L),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 10L, "Me_2", 2.0D, "min_Me_1", 5L),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 5L, "Me_2", 7.0D, "min_Me_1", 5L)
        );

    }

    @Test
    public void testAnMaxWithCalcClause() throws ScriptException {

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
        context.setAttribute("ds1", anCountDS1, ScriptContext.ENGINE_SCOPE);


        engine.eval("res := ds1 [ calc max_Me_1:= max ( Me_1 over ( partition by Id_1,Id_2 order by Year) )];");
        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);

        /*
        *
        +----+----+----+----+----+--------+
        |Id_1|Id_2|Year|Me_1|Me_2|max_Me_1|
        +----+----+----+----+----+--------+
        |   A|  XX|2000|   3| 1.0|       3|
        |   A|  XX|2001|   4| 9.0|       4|
        |   A|  XX|2002|   7| 5.0|       7|
        |   A|  XX|2003|   6| 8.0|       7|
        |   A|  YY|2000|   9| 3.0|       9|
        |   A|  YY|2001|   5| 4.0|       9|
        |   A|  YY|2002|  10| 2.0|      10|
        |   A|  YY|2003|   5| 7.0|      10|
        +----+----+----+----+----+--------+
        * */
        List<Map<String, Object>> actual = ((Dataset) engine.getContext().getAttribute("res")).getDataAsMap();

        assertThat(actual).containsExactly(
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 3L, "Me_2", 1.0D, "max_Me_1", 3L),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 4L, "Me_2", 9.0D, "max_Me_1", 4L),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 7L, "Me_2", 5.0D, "max_Me_1", 7L),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 6L, "Me_2", 8.0D, "max_Me_1", 7L),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 9L, "Me_2", 3.0D, "max_Me_1", 9L),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 5L, "Me_2", 4.0D, "max_Me_1", 9L),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 10L, "Me_2", 2.0D, "max_Me_1", 10L),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 5L, "Me_2", 7.0D, "max_Me_1", 10L)
        );

    }

    @Test
    public void testAnAvgWithCalcClause() throws ScriptException {

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
        context.setAttribute("ds1", anCountDS1, ScriptContext.ENGINE_SCOPE);


        engine.eval("res := ds1 [ calc avg_Me_1:= avg ( Me_1 over ( partition by Id_1,Id_2 order by Year) )];");
        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);

        /*
        *
        +----+----+----+----+----+-----------------+
        |Id_1|Id_2|Year|Me_1|Me_2|         avg_Me_1|
        +----+----+----+----+----+-----------------+
        |   A|  XX|2000|   3| 1.0|              3.0|
        |   A|  XX|2001|   4| 9.0|              3.5|
        |   A|  XX|2002|   7| 5.0|4.666666666666667|
        |   A|  XX|2003|   6| 8.0|              5.0|
        |   A|  YY|2000|   9| 3.0|              9.0|
        |   A|  YY|2001|   5| 4.0|              7.0|
        |   A|  YY|2002|  10| 2.0|              8.0|
        |   A|  YY|2003|   5| 7.0|             7.25|
        +----+----+----+----+----+-----------------+
        * */
        List<Map<String, Object>> actual = ((Dataset) engine.getContext().getAttribute("res")).getDataAsMap();

        assertThat(actual).containsExactly(
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 3L, "Me_2", 1.0D, "avg_Me_1", 3.0D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 4L, "Me_2", 9.0D, "avg_Me_1", 3.5D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 7L, "Me_2", 5.0D, "avg_Me_1", 4.666666666666667D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 6L, "Me_2", 8.0D, "avg_Me_1", 5.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 9L, "Me_2", 3.0D, "avg_Me_1", 9.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 5L, "Me_2", 4.0D, "avg_Me_1", 7.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 10L, "Me_2", 2.0D, "avg_Me_1", 8.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 5L, "Me_2", 7.0D, "avg_Me_1", 7.25D)
        );

    }

    @Test
    public void testAnMedianWithCalcClause() throws ScriptException {

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
        context.setAttribute("ds1", anCountDS1, ScriptContext.ENGINE_SCOPE);


        engine.eval("res := ds1 [ calc median_Me_1:= median ( Me_1 over ( partition by Id_1,Id_2 order by Year) )];");
        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);

        /*
        +----+----+----+----+----+-----------+
        |Id_1|Id_2|Year|Me_1|Me_2|median_Me_1|
        +----+----+----+----+----+-----------+
        |   A|  XX|2000|   3| 1.0|          3|
        |   A|  XX|2001|   4| 9.0|          3|
        |   A|  XX|2002|   7| 5.0|          4|
        |   A|  XX|2003|   6| 8.0|          4|
        |   A|  YY|2000|   9| 3.0|          9|
        |   A|  YY|2001|   5| 4.0|          5|
        |   A|  YY|2002|  10| 2.0|          9|
        |   A|  YY|2003|   5| 7.0|          5|
        +----+----+----+----+----+-----------+
        * */
        List<Map<String, Object>> actual = ((Dataset) engine.getContext().getAttribute("res")).getDataAsMap();

        assertThat(actual).containsExactly(
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 3L, "Me_2", 1.0D, "median_Me_1", 3L),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 4L, "Me_2", 9.0D, "median_Me_1", 3L),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 7L, "Me_2", 5.0D, "median_Me_1", 4L),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 6L, "Me_2", 8.0D, "median_Me_1", 4L),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 9L, "Me_2", 3.0D, "median_Me_1", 9L),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 5L, "Me_2", 4.0D, "median_Me_1", 5L),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 10L, "Me_2", 2.0D, "median_Me_1", 9L),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 5L, "Me_2", 7.0D, "median_Me_1", 5L)
        );

    }

    @Test
    public void testAnStdPopWithCalcClause() throws ScriptException {

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
        context.setAttribute("ds1", anCountDS1, ScriptContext.ENGINE_SCOPE);


        engine.eval("res := ds1 [ calc stddev_pop_Me_1:= stddev_pop ( Me_1 over ( partition by Id_1,Id_2 order by Year) )];");
        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);

        /*
        *
        +----+----+----+----+----+------------------+
        |Id_1|Id_2|Year|Me_1|Me_2|   stddev_pop_Me_1|
        +----+----+----+----+----+------------------+
        |   A|  XX|2000|   3| 1.0|               0.0|
        |   A|  XX|2001|   4| 9.0|               0.5|
        |   A|  XX|2002|   7| 5.0| 1.699673171197595|
        |   A|  XX|2003|   6| 8.0|1.5811388300841895|
        |   A|  YY|2000|   9| 3.0|               0.0|
        |   A|  YY|2001|   5| 4.0|               2.0|
        |   A|  YY|2002|  10| 2.0| 2.160246899469287|
        |   A|  YY|2003|   5| 7.0| 2.277608394786075|
        +----+----+----+----+----+------------------+
        * */
        List<Map<String, Object>> actual = ((Dataset) engine.getContext().getAttribute("res")).getDataAsMap();

        assertThat(actual).containsExactly(
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 3L, "Me_2", 1.0D, "stddev_pop_Me_1", 0.0D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 4L, "Me_2", 9.0D, "stddev_pop_Me_1", 0.5D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 7L, "Me_2", 5.0D, "stddev_pop_Me_1", 1.699673171197595D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 6L, "Me_2", 8.0D, "stddev_pop_Me_1", 1.5811388300841895D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 9L, "Me_2", 3.0D, "stddev_pop_Me_1", 0.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 5L, "Me_2", 4.0D, "stddev_pop_Me_1", 2.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 10L, "Me_2", 2.0D, "stddev_pop_Me_1", 2.160246899469287D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 5L, "Me_2", 7.0D, "stddev_pop_Me_1", 2.277608394786075D)
        );

    }

    @Test
    public void testAnStdSampWithCalcClause() throws ScriptException {

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
        context.setAttribute("ds1", anCountDS1, ScriptContext.ENGINE_SCOPE);


        engine.eval("res := ds1 [ calc stddev_samp_Me_1:= stddev_samp ( Me_1 over ( partition by Id_1,Id_2 order by Year) )];");
        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);

        /*
        *
        +----+----+----+----+----+------------------+
        |Id_1|Id_2|Year|Me_1|Me_2|  stddev_samp_Me_1|
        +----+----+----+----+----+------------------+
        |   A|  XX|2000|   3| 1.0|              null|
        |   A|  XX|2001|   4| 9.0|0.7071067811865476|
        |   A|  XX|2002|   7| 5.0|2.0816659994661326|
        |   A|  XX|2003|   6| 8.0|1.8257418583505536|
        |   A|  YY|2000|   9| 3.0|              null|
        |   A|  YY|2001|   5| 4.0|2.8284271247461903|
        |   A|  YY|2002|  10| 2.0|2.6457513110645907|
        |   A|  YY|2003|   5| 7.0|2.6299556396765835|
        +----+----+----+----+----+------------------+
        * */
        List<Map<String, Object>> actualWithNull = ((Dataset) engine.getContext().getAttribute("res")).getDataAsMap();

        List<Map<String, Object>> actual = new ArrayList<>();
        for(Map<String, Object> map:actualWithNull){
            actual.add( replaceNullValues(map, DEFAULT_NULL_STR));
        }



        assertThat(actual).containsExactly(
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 3L, "Me_2", 1.0D, "stddev_samp_Me_1", "null"),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 4L, "Me_2", 9.0D, "stddev_samp_Me_1", 0.7071067811865476D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 7L, "Me_2", 5.0D, "stddev_samp_Me_1", 2.0816659994661326D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 6L, "Me_2", 8.0D, "stddev_samp_Me_1", 1.8257418583505536D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 9L, "Me_2", 3.0D, "stddev_samp_Me_1", "null"),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 5L, "Me_2", 4.0D, "stddev_samp_Me_1", 2.8284271247461903D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 10L, "Me_2", 2.0D, "stddev_samp_Me_1", 2.6457513110645907D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 5L, "Me_2", 7.0D, "stddev_samp_Me_1", 2.6299556396765835D)
        );

    }

    @Test
    public void testAnVarPopWithCalcClause() throws ScriptException {

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
        context.setAttribute("ds1", anCountDS1, ScriptContext.ENGINE_SCOPE);


        engine.eval("res := ds1 [ calc var_pop_Me_1:= var_pop ( Me_1 over ( partition by Id_1,Id_2 order by Year) )];");
        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);

        /*
        *
        +----+----+----+----+----+------------------+
        |Id_1|Id_2|Year|Me_1|Me_2|      var_pop_Me_1|
        +----+----+----+----+----+------------------+
        |   A|  XX|2000|   3| 1.0|               0.0|
        |   A|  XX|2001|   4| 9.0|              0.25|
        |   A|  XX|2002|   7| 5.0| 2.888888888888889|
        |   A|  XX|2003|   6| 8.0|2.4999999999999996|
        |   A|  YY|2000|   9| 3.0|               0.0|
        |   A|  YY|2001|   5| 4.0|               4.0|
        |   A|  YY|2002|  10| 2.0| 4.666666666666667|
        |   A|  YY|2003|   5| 7.0|            5.1875|
        +----+----+----+----+----+------------------+
        * */
        List<Map<String, Object>> actual = ((Dataset) engine.getContext().getAttribute("res")).getDataAsMap();

        assertThat(actual).containsExactly(
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 3L, "Me_2", 1.0D, "var_pop_Me_1", 0.0D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 4L, "Me_2", 9.0D, "var_pop_Me_1", 0.25D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 7L, "Me_2", 5.0D, "var_pop_Me_1", 2.888888888888889D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 6L, "Me_2", 8.0D, "var_pop_Me_1", 2.4999999999999996D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 9L, "Me_2", 3.0D, "var_pop_Me_1", 0.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 5L, "Me_2", 4.0D, "var_pop_Me_1", 4.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 10L, "Me_2", 2.0D, "var_pop_Me_1", 4.666666666666667D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 5L, "Me_2", 7.0D, "var_pop_Me_1", 5.1875D)
        );

    }

    @Test
    public void testAnVarSampWithCalcClause() throws ScriptException {

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
        context.setAttribute("ds1", anCountDS1, ScriptContext.ENGINE_SCOPE);


        engine.eval("res := ds1 [ calc var_samp_Me_1:= var_samp ( Me_1 over ( partition by Id_1,Id_2 order by Year) )];");
        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);

        /*
        *
        +----+----+----+----+----+------------------+
        |Id_1|Id_2|Year|Me_1|Me_2|     var_samp_Me_1|
        +----+----+----+----+----+------------------+
        |   A|  XX|2000|   3| 1.0|              null|
        |   A|  XX|2001|   4| 9.0|               0.5|
        |   A|  XX|2002|   7| 5.0| 4.333333333333333|
        |   A|  XX|2003|   6| 8.0|3.3333333333333326|
        |   A|  YY|2000|   9| 3.0|              null|
        |   A|  YY|2001|   5| 4.0|               8.0|
        |   A|  YY|2002|  10| 2.0|               7.0|
        |   A|  YY|2003|   5| 7.0| 6.916666666666667|
        +----+----+----+----+----+------------------+
        * */
        List<Map<String, Object>> actualWithNull = ((Dataset) engine.getContext().getAttribute("res")).getDataAsMap();

        List<Map<String, Object>> actual = new ArrayList<>();
        for(Map<String, Object> map:actualWithNull){
            actual.add( replaceNullValues(map, DEFAULT_NULL_STR));
        }

        assertThat(actual).containsExactly(Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 3L, "Me_2", 1.0D, "var_samp_Me_1", "null"),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 4L, "Me_2", 9.0D, "var_samp_Me_1", 0.5D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 7L, "Me_2", 5.0D, "var_samp_Me_1", 4.333333333333333D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 6L, "Me_2", 8.0D, "var_samp_Me_1", 3.3333333333333326D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 9L, "Me_2", 3.0D, "var_samp_Me_1", "null"),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 5L, "Me_2", 4.0D, "var_samp_Me_1", 8.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 10L, "Me_2", 2.0D, "var_samp_Me_1", 7.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 5L, "Me_2", 7.0D, "var_samp_Me_1", 6.916666666666667D)
        );

    }

    @Test
    public void testAnLeadWithCalcClause() throws ScriptException {

        // Analytical function Test case 1 : lead on window with partition, order by and range
        /* Input dataset
        +----+----+----+----+----+
        |Id_1|Id_2|Year|Me_1|Me_2|
        +----+----+----+----+----+
        |   A|  XX|1993|   3| 1.0|
        |   A|  XX|1994|   4| 9.0|
        |   A|  XX|1995|   7| 5.0|
        |   A|  XX|1996|   6| 8.0|
        |   A|  YY|1993|   9| 3.0|
        |   A|  YY|1994|   5| 4.0|
        |   A|  YY|1995|  10| 2.0|
        |   A|  YY|1996|   2| 7.0|
        +----+----+----+----+----+
        * */
        ScriptContext context = engine.getContext();
        context.setAttribute("ds2", anCountDS2, ScriptContext.ENGINE_SCOPE);
        engine.eval("res := ds2 [ calc lead_Me_1 := lead ( Me_1 , 1 over ( partition by Id_1 , Id_2 order by Year ) )] ;");
        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);

        /*
        *
        +----+----+----+----+----+---------+
        |Id_1|Id_2|Year|Me_1|Me_2|lead_Me_1|
        +----+----+----+----+----+---------+
        |   A|  XX|1993|   3| 1.0|        4|
        |   A|  XX|1994|   4| 9.0|        7|
        |   A|  XX|1995|   7| 5.0|        6|
        |   A|  XX|1996|   6| 8.0|     null|
        |   A|  YY|1993|   9| 3.0|        5|
        |   A|  YY|1994|   5| 4.0|       10|
        |   A|  YY|1995|  10| 2.0|        2|
        |   A|  YY|1996|   2| 7.0|     null|
        +----+----+----+----+----+---------+
        * */

        List<Map<String, Object>> actualWithNull = ((Dataset) engine.getContext().getAttribute("res")).getDataAsMap();

        List<Map<String, Object>> actual = new ArrayList<>();
        for(Map<String, Object> map:actualWithNull){
            actual.add( replaceNullValues(map, DEFAULT_NULL_STR));
        }

        assertThat(actual).containsExactly(
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 1993L, "Me_1", 3L, "Me_2", 1.0D, "lead_Me_1", 4L),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 1994L, "Me_1", 4L, "Me_2", 9.0D, "lead_Me_1", 7L),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 1995L, "Me_1", 7L, "Me_2", 5.0D, "lead_Me_1", 6L),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 1996L, "Me_1", 6L, "Me_2", 8.0D, "lead_Me_1", "null"),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 1993L, "Me_1", 9L, "Me_2", 3.0D, "lead_Me_1", 5L),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 1994L, "Me_1", 5L, "Me_2", 4.0D, "lead_Me_1", 10L),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 1995L, "Me_1", 10L, "Me_2", 2.0D, "lead_Me_1", 2L),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 1996L, "Me_1", 2L, "Me_2", 7.0D, "lead_Me_1", "null")
        );

    }

    @Test
    public void testAnLagWithCalcClause() throws ScriptException {

        // Analytical function Test case 1 : lead on window with partition, order by and range
        /* Input dataset
        +----+----+----+----+----+
        |Id_1|Id_2|Year|Me_1|Me_2|
        +----+----+----+----+----+
        |   A|  XX|1993|   3| 1.0|
        |   A|  XX|1994|   4| 9.0|
        |   A|  XX|1995|   7| 5.0|
        |   A|  XX|1996|   6| 8.0|
        |   A|  YY|1993|   9| 3.0|
        |   A|  YY|1994|   5| 4.0|
        |   A|  YY|1995|  10| 2.0|
        |   A|  YY|1996|   2| 7.0|
        +----+----+----+----+----+
        * */
        ScriptContext context = engine.getContext();
        context.setAttribute("ds2", anCountDS2, ScriptContext.ENGINE_SCOPE);
        engine.eval("res := ds2 [ calc lag_Me_1 := lag ( Me_1 , 1 over ( partition by Id_1 , Id_2 order by Year ) )] ;");
        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);

        /*
        *
        +----+----+----+----+----+--------+
        |Id_1|Id_2|Year|Me_1|Me_2|lag_Me_1|
        +----+----+----+----+----+--------+
        |   A|  XX|1993|   3| 1.0|    null|
        |   A|  XX|1994|   4| 9.0|       3|
        |   A|  XX|1995|   7| 5.0|       4|
        |   A|  XX|1996|   6| 8.0|       7|
        |   A|  YY|1993|   9| 3.0|    null|
        |   A|  YY|1994|   5| 4.0|       9|
        |   A|  YY|1995|  10| 2.0|       5|
        |   A|  YY|1996|   2| 7.0|      10|
        +----+----+----+----+----+--------+
        * */

        List<Map<String, Object>> actualWithNull = ((Dataset) engine.getContext().getAttribute("res")).getDataAsMap();

        List<Map<String, Object>> actual = new ArrayList<>();
        for(Map<String, Object> map:actualWithNull){
            actual.add( replaceNullValues(map, DEFAULT_NULL_STR));
        }
        assertThat(actual).containsExactly(
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 1993L, "Me_1", 3L, "Me_2", 1.0D, "lag_Me_1", "null"),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 1994L, "Me_1", 4L, "Me_2", 9.0D, "lag_Me_1", 3L),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 1995L, "Me_1", 7L, "Me_2", 5.0D, "lag_Me_1", 4L),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 1996L, "Me_1", 6L, "Me_2", 8.0D, "lag_Me_1", 7L),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 1993L, "Me_1", 9L, "Me_2", 3.0D, "lag_Me_1", "null"),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 1994L, "Me_1", 5L, "Me_2", 4.0D, "lag_Me_1", 9L),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 1995L, "Me_1", 10L, "Me_2", 2.0D, "lag_Me_1", 5L),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 1996L, "Me_1", 2L, "Me_2", 7.0D, "lag_Me_1", 10L)
        );

    }

    @Test
    public void testAnRatioToReportWithCalcClause() throws ScriptException {

        InMemoryDataset anDS = new InMemoryDataset(
                List.of(
                        Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 3L, "Me_2", 1D),
                        Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 4L, "Me_2", 3D),
                        Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 7L, "Me_2", 5D),
                        Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 6L, "Me_2", 1D),
                        Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 12L, "Me_2", 0D),
                        Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 8L, "Me_2", 8D),
                        Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 6L, "Me_2", 5D),
                        Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 14L, "Me_2", -3D)

                ),
                Map.of("Id_1", String.class, "Id_2", String.class, "Year", Long.class, "Me_1", Long.class, "Me_2", Double.class),
                Map.of("Id_1", Role.IDENTIFIER, "Id_2", Role.IDENTIFIER, "Year", Role.IDENTIFIER, "Me_1", Role.MEASURE, "Me_2", Role.MEASURE)
        );
        /* Input dataset
        +----+----+----+----+----+
        |Id_1|Id_2|Id_3|Me_1|Me_2|
        +----+----+----+----+----+
        |   A|  XX|2000|   3|   1|
        |   A|  XX|2001|   4|   3|
        |   A|  XX|2002|   7|   5|
        |   A|  XX|2003|   6|   1|
        |   A|  YY|2000|  12|   0|
        |   A|  YY|2001|   8|   8|
        |   A|  YY|2002|   6|   5|
        |   A|  YY|2003|  14|  -3|
        +----+----+----+----+----+
        * */
        ScriptContext context = engine.getContext();
        context.setAttribute("ds", anDS, ScriptContext.ENGINE_SCOPE);

        engine.eval("res :=  ds  [ calc ratio_Me_1 := ratio_to_report ( Me_1 over ( partition by Id_1, Id_2 ) )];");
        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);

        /*
        *
        +----+----+----+----+----+----------+
        |Id_1|Id_2|Id_3|Me_1|Me_2|ratio_Me_1|
        +----+----+----+----+----+----------+
        |   A|  XX|2000|   3|   1|      0.15|
        |   A|  XX|2001|   4|   3|       0.2|
        |   A|  XX|2002|   7|   5|      0.35|
        |   A|  XX|2003|   6|   1|       0.3|
        |   A|  YY|2000|  12|   0|       0.3|
        |   A|  YY|2001|   8|   8|       0.2|
        |   A|  YY|2002|   6|   5|      0.15|
        |   A|  YY|2003|  14|  -3|      0.35|
        +----+----+----+----+----+----------+
        * */
        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 3L, "Me_2", 1.0D, "ratio_Me_1", 0.15D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 4L, "Me_2", 3.0D, "ratio_Me_1", 0.2D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 7L, "Me_2", 5.0D, "ratio_Me_1", 0.35D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 6L, "Me_2", 1.0D, "ratio_Me_1", 0.3D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 12L, "Me_2", 0.0D, "ratio_Me_1", 0.3D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 8L, "Me_2", 8.0D, "ratio_Me_1", 0.2D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 6L, "Me_2", 5.0D, "ratio_Me_1", 0.15D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 14L, "Me_2", -3.0D, "ratio_Me_1", 0.35D)
        );

    }
    /*
     * End test with calc statement*/


    /***********************************Unit test for implicit analytic function *************************************/

//    /*
//     * Test case for analytic function Count
//     *
//     * */
//
//    @Test
//    public void testAnCountWithPartitionClause() throws ScriptException {
//
//        // Analytical function Test case 1 : count on window with partition
//        /* Input dataset
//        *   +----+----+----+----+----+
//            |Id_1|Id_2|Year|Me_1|Me_2|
//            +----+----+----+----+----+
//            |   A|  XX|2000|   3| 1.0|
//            |   A|  XX|2001|   4| 9.0|
//            |   A|  XX|2002|   7| 5.0|
//            |   A|  XX|2003|   6| 8.0|
//            |   A|  YY|2000|   9| 3.0|
//            |   A|  YY|2001|   5| 4.0|
//            |   A|  YY|2002|  10| 2.0|
//            |   A|  YY|2003|   5| 7.0|
//            +----+----+----+----+----+
//        * */
//        ScriptContext context = engine.getContext();
//        context.setAttribute("ds1", anCountDS1, ScriptContext.ENGINE_SCOPE);
//
//
//        engine.eval("res := count ( ds1 over ( partition by Id_1) );");
//        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);
//
//        /*
//        * The result data frame need to check mutable or not mutable on Mesaument column
//        *
//        *   +----+----+----+----+----+----------+----------+
//            |Id_1|Id_2|Year|Me_1|Me_2|count_Me_1|count_Me_2|
//            +----+----+----+----+----+----------+----------+
//            |   A|  XX|2000|   3| 1.0|         8|         8|
//            |   A|  XX|2001|   4| 9.0|         8|         8|
//            |   A|  XX|2002|   7| 5.0|         8|         8|
//            |   A|  XX|2003|   6| 8.0|         8|         8|
//            |   A|  YY|2000|   9| 3.0|         8|         8|
//            |   A|  YY|2001|   5| 4.0|         8|         8|
//            |   A|  YY|2002|  10| 2.0|         8|         8|
//            |   A|  YY|2003|   5| 7.0|         8|         8|
//            +----+----+----+----+----+----------+----------+
//        * */
//        List<Map<String, Object>> actual = ((Dataset) engine.getContext().getAttribute("res")).getDataAsMap();
//
//        assertThat(actual).containsExactly(
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 8L, "Me_2", 8L),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 8L, "Me_2", 8L),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 8L, "Me_2", 8L),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 8L, "Me_2", 8L),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 8L, "Me_2", 8L),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 8L, "Me_2", 8L),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 8L, "Me_2", 8L),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 8L, "Me_2", 8L)
//        );
//
//    }
//
//    @Test
//    public void testAnCountWithPartitionOrderByClause() throws ScriptException {
//
//        // Analytical function Test case 2 : count on window with partition and orderBy
//        /* Input dataset
//        *   +----+----+----+----+----+
//            |Id_1|Id_2|Year|Me_1|Me_2|
//            +----+----+----+----+----+
//            |   A|  XX|2000|   3| 1.0|
//            |   A|  XX|2001|   4| 9.0|
//            |   A|  XX|2002|   7| 5.0|
//            |   A|  XX|2003|   6| 8.0|
//            |   A|  YY|2000|   9| 3.0|
//            |   A|  YY|2001|   5| 4.0|
//            |   A|  YY|2002|  10| 2.0|
//            |   A|  YY|2003|   5| 7.0|
//            +----+----+----+----+----+
//        * */
//        ScriptContext context = engine.getContext();
//        context.setAttribute("ds1", anCountDS1, ScriptContext.ENGINE_SCOPE);
//
//
//        engine.eval("res := count ( ds1 over ( partition by Id_1 order by Id_2) );");
//        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);
//
//        /*
//        * The result data frame
//        *
//        *   +----+----+----+----+----+----------+----------+
//            |Id_1|Id_2|Year|Me_1|Me_2|count_Me_1|count_Me_2|
//            +----+----+----+----+----+----------+----------+
//            |   A|  XX|2000|   3| 1.0|         4|         4|
//            |   A|  XX|2001|   4| 9.0|         4|         4|
//            |   A|  XX|2002|   7| 5.0|         4|         4|
//            |   A|  XX|2003|   6| 8.0|         4|         4|
//            |   A|  YY|2000|   9| 3.0|         8|         8|
//            |   A|  YY|2001|   5| 4.0|         8|         8|
//            |   A|  YY|2002|  10| 2.0|         8|         8|
//            |   A|  YY|2003|   5| 7.0|         8|         8|
//            +----+----+----+----+----+----------+----------+
//        * */
//        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 4L, "Me_2", 4L),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 4L, "Me_2", 4L),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 4L, "Me_2", 4L),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 4L, "Me_2", 4L),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 8L, "Me_2", 8L),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 8L, "Me_2", 8L),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 8L, "Me_2", 8L),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 8L, "Me_2", 8L)
//        );
//
//    }
//
//    @Test
//    public void testAnCountWithPartitionOrderByDPClause() throws ScriptException {
//
//        // Analytical function count test case 3 : count on window with partition, orderBy and data points
//        /* Input dataset
//        *   +----+----+----+----+----+
//            |Id_1|Id_2|Year|Me_1|Me_2|
//            +----+----+----+----+----+
//            |   A|  XX|2000|   3| 1.0|
//            |   A|  XX|2001|   4| 9.0|
//            |   A|  XX|2002|   7| 5.0|
//            |   A|  XX|2003|   6| 8.0|
//            |   A|  YY|2000|   9| 3.0|
//            |   A|  YY|2001|   5| 4.0|
//            |   A|  YY|2002|  10| 2.0|
//            |   A|  YY|2003|   5| 7.0|
//            +----+----+----+----+----+
//        * */
//        ScriptContext context = engine.getContext();
//        context.setAttribute("ds1", anCountDS1, ScriptContext.ENGINE_SCOPE);
//
//        engine.eval("res := ds1 [ calc count_Me_1:= count ( Me_1 over ( partition by Id_1 order by Id_2 data points between 2 preceding and 2 following) )];");
//        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);
//
//        /*
//        * The result data frame
//        *
//        *   +----+----+----+----+----+----------+----------+
//            |Id_1|Id_2|Year|Me_1|Me_2|count_Me_1|count_Me_2|
//            +----+----+----+----+----+----------+----------+
//            |   A|  XX|2000|   3| 1.0|         3|         3|
//            |   A|  XX|2001|   4| 9.0|         4|         4|
//            |   A|  XX|2002|   7| 5.0|         5|         5|
//            |   A|  XX|2003|   6| 8.0|         5|         5|
//            |   A|  YY|2000|   9| 3.0|         5|         5|
//            |   A|  YY|2001|   5| 4.0|         5|         5|
//            |   A|  YY|2002|  10| 2.0|         4|         4|
//            |   A|  YY|2003|   5| 7.0|         3|         3|
//            +----+----+----+----+----+----------+----------+
//
//        * */
//        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 3L, "Me_2", 3L),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 4L, "Me_2", 4L),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 5L, "Me_2", 5L),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 5L, "Me_2", 5L),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 5L, "Me_2", 5L),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 5L, "Me_2", 5L),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 4L, "Me_2", 4L),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 3L, "Me_2", 3L)
//        );
//
//    }
//
//    @Test
//    public void testAnCountWithPartitionOrderByRangeClause() throws ScriptException {
//
//        // Analytical function count test case 4 : count on window with partition, orderBy and range
//        // Because range build window based on the ordered column value, and Id_2 is string, so we change
//        // the order by column to year.
//        /* Input dataset
//        *   +----+----+----+----+----+
//            |Id_1|Id_2|Year|Me_1|Me_2|
//            +----+----+----+----+----+
//            |   A|  XX|2000|   3| 1.0|
//            |   A|  XX|2001|   4| 9.0|
//            |   A|  XX|2002|   7| 5.0|
//            |   A|  XX|2003|   6| 8.0|
//            |   A|  YY|2000|   9| 3.0|
//            |   A|  YY|2001|   5| 4.0|
//            |   A|  YY|2002|  10| 2.0|
//            |   A|  YY|2003|   5| 7.0|
//            +----+----+----+----+----+
//        * */
//        ScriptContext context = engine.getContext();
//        context.setAttribute("ds1", anCountDS1, ScriptContext.ENGINE_SCOPE);
//
//
//        engine.eval("res := count ( ds1 over ( partition by Id_1 order by Year range between -1 and 1) );");
//        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);
//
//        /*
//        * The result data frame
//        *
//        *   +----+----+----+----+----+----------+----------+
//            |Id_1|Id_2|Year|Me_1|Me_2|count_Me_1|count_Me_2|
//            +----+----+----+----+----+----------+----------+
//            |   A|  XX|2000|   3| 1.0|         4|         4|
//            |   A|  YY|2000|   9| 3.0|         4|         4|
//            |   A|  XX|2001|   4| 9.0|         6|         6|
//            |   A|  YY|2001|   5| 4.0|         6|         6|
//            |   A|  XX|2002|   7| 5.0|         6|         6|
//            |   A|  YY|2002|  10| 2.0|         6|         6|
//            |   A|  XX|2003|   6| 8.0|         4|         4|
//            |   A|  YY|2003|   5| 7.0|         4|         4|
//            +----+----+----+----+----+----------+----------+
//
//        * */
//        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 4L, "Me_2", 4L),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 4L, "Me_2", 4L),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 6L, "Me_2", 6L),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 6L, "Me_2", 6L),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 6L, "Me_2", 6L),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 6L, "Me_2", 6L),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 4L, "Me_2", 4L),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 4L, "Me_2", 4L)
//        );
//
//    }
//
//    /*
//     * End of count test case */
//
//
//    /*
//     * Test case for analytic function Sum
//     *
//     * */
//    @Test
//    public void testAnSumWithPartitionClause() throws ScriptException {
//
//        // Analytical function Test case 1 : sum on window with partition
//        /* Input dataset
//        *   +----+----+----+----+----+
//            |Id_1|Id_2|Year|Me_1|Me_2|
//            +----+----+----+----+----+
//            |   A|  XX|2000|   3| 1.0|
//            |   A|  XX|2001|   4| 9.0|
//            |   A|  XX|2002|   7| 5.0|
//            |   A|  XX|2003|   6| 8.0|
//            |   A|  YY|2000|   9| 3.0|
//            |   A|  YY|2001|   5| 4.0|
//            |   A|  YY|2002|  10| 2.0|
//            |   A|  YY|2003|   5| 7.0|
//            +----+----+----+----+----+
//        * */
//        ScriptContext context = engine.getContext();
//        context.setAttribute("ds1", anCountDS1, ScriptContext.ENGINE_SCOPE);
//
//
//        engine.eval("res := sum ( ds1 over ( partition by Id_1, Id_2 ) );");
//        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);
//
//        /*
//        * The result data frame need to check mutable or not mutable on Mesaument column
//        *
//        *   +----+----+----+----+----+----------+----------+
//            |Id_1|Id_2|Year|Me_1|Me_2|sum_Me_1|sum_Me_2|
//            +----+----+----+----+----+--------+--------+
//            |   A|  XX|2000|   3| 1.0|      20|    23.0|
//            |   A|  XX|2001|   4| 9.0|      20|    23.0|
//            |   A|  XX|2002|   7| 5.0|      20|    23.0|
//            |   A|  XX|2003|   6| 8.0|      20|    23.0|
//            |   A|  YY|2000|   9| 3.0|      29|    16.0|
//            |   A|  YY|2001|   5| 4.0|      29|    16.0|
//            |   A|  YY|2002|  10| 2.0|      29|    16.0|
//            |   A|  YY|2003|   5| 7.0|      29|    16.0|
//            +----+----+----+----+----+--------+--------+
//        * */
//        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 20L, "Me_2", 23.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 20L, "Me_2", 23.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 20L, "Me_2", 23.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 20L, "Me_2", 23.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 29L, "Me_2", 16.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 29L, "Me_2", 16.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 29L, "Me_2", 16.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 29L, "Me_2", 16.0D)
//        );
//
//    }
//
//    @Test
//    public void testAnSumWithPartitionOrderByClause() throws ScriptException {
//
//        // Analytical function Test case 2 : sum on window with partition and order by
//        /* Input dataset
//        *   +----+----+----+----+----+
//            |Id_1|Id_2|Year|Me_1|Me_2|
//            +----+----+----+----+----+
//            |   A|  XX|2000|   3| 1.0|
//            |   A|  XX|2001|   4| 9.0|
//            |   A|  XX|2002|   7| 5.0|
//            |   A|  XX|2003|   6| 8.0|
//            |   A|  YY|2000|   9| 3.0|
//            |   A|  YY|2001|   5| 4.0|
//            |   A|  YY|2002|  10| 2.0|
//            |   A|  YY|2003|   5| 7.0|
//            +----+----+----+----+----+
//        * */
//        ScriptContext context = engine.getContext();
//        context.setAttribute("ds1", anCountDS1, ScriptContext.ENGINE_SCOPE);
//
//
//        engine.eval("res := sum ( ds1 over ( partition by Id_1 order by Id_2) );");
//        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);
//
//        /*
//        * The result data frame need to check mutable or not mutable on Mesaument column
//        *
//        *   +----+----+----+----+----+----------+----------+
//            |Id_1|Id_2|Year|Me_1|Me_2|sum_Me_1|sum_Me_2|
//            +----+----+----+----+----+--------+--------+
//            |   A|  XX|2000|   3| 1.0|      20|    23.0|
//            |   A|  XX|2001|   4| 9.0|      20|    23.0|
//            |   A|  XX|2002|   7| 5.0|      20|    23.0|
//            |   A|  XX|2003|   6| 8.0|      20|    23.0|
//            |   A|  YY|2000|   9| 3.0|      49|    39.0|
//            |   A|  YY|2001|   5| 4.0|      49|    39.0|
//            |   A|  YY|2002|  10| 2.0|      49|    39.0|
//            |   A|  YY|2003|   5| 7.0|      49|    39.0|
//            +----+----+----+----+----+--------+--------+
//
//        * */
//        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 20L, "Me_2", 23.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 20L, "Me_2", 23.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 20L, "Me_2", 23.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 20L, "Me_2", 23.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 49L, "Me_2", 39.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 49L, "Me_2", 39.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 49L, "Me_2", 39.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 49L, "Me_2", 39.0D)
//        );
//
//    }
//
//    @Test
//    public void testAnSumWithOrderByClause() throws ScriptException {
//
//        // Analytical function Test case 3 : sum on window with only order by without partition
//        /* Input dataset
//        *   +----+----+----+----+----+
//            |Id_1|Id_2|Year|Me_1|Me_2|
//            +----+----+----+----+----+
//            |   A|  XX|2000|   3| 1.0|
//            |   A|  XX|2001|   4| 9.0|
//            |   A|  XX|2002|   7| 5.0|
//            |   A|  XX|2003|   6| 8.0|
//            |   A|  YY|2000|   9| 3.0|
//            |   A|  YY|2001|   5| 4.0|
//            |   A|  YY|2002|  10| 2.0|
//            |   A|  YY|2003|   5| 7.0|
//            +----+----+----+----+----+
//        * */
//        ScriptContext context = engine.getContext();
//        context.setAttribute("ds1", anCountDS1, ScriptContext.ENGINE_SCOPE);
//
//
//        engine.eval("res := sum ( ds1 over ( order by Id_1, Id_2, Year ) );");
//        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);
//
//        /*
//        * The result data frame need to check mutable or not mutable on Mesaument column
//        *
//        *   +----+----+----+----+----+----------+----------+
//            |Id_1|Id_2|Year|Me_1|Me_2|sum_Me_1|sum_Me_2|
//            +----+----+----+----+----+--------+--------+
//            |   A|  XX|2000|   3| 1.0|       3|     1.0|
//            |   A|  XX|2001|   4| 9.0|       7|    10.0|
//            |   A|  XX|2002|   7| 5.0|      14|    15.0|
//            |   A|  XX|2003|   6| 8.0|      20|    23.0|
//            |   A|  YY|2000|   9| 3.0|      29|    26.0|
//            |   A|  YY|2001|   5| 4.0|      34|    30.0|
//            |   A|  YY|2002|  10| 2.0|      44|    32.0|
//            |   A|  YY|2003|   5| 7.0|      49|    39.0|
//            +----+----+----+----+----+--------+--------+
//
//        * */
//        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 3L, "Me_2", 1.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 7L, "Me_2", 10.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 14L, "Me_2", 15.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 20L, "Me_2", 23.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 29L, "Me_2", 26.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 34L, "Me_2", 30.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 44L, "Me_2", 32.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 49L, "Me_2", 39.0D)
//        );
//
//    }
//
//    @Test
//    public void testAnSumWithPartitionOrderByDPClause() throws ScriptException {
//
//        // Analytical function count test case 4 : sum on window with partition, orderBy and data points
//        /* Input dataset
//        *   +----+----+----+----+----+
//            |Id_1|Id_2|Year|Me_1|Me_2|
//            +----+----+----+----+----+
//            |   A|  XX|2000|   3| 1.0|
//            |   A|  XX|2001|   4| 9.0|
//            |   A|  XX|2002|   7| 5.0|
//            |   A|  XX|2003|   6| 8.0|
//            |   A|  YY|2000|   9| 3.0|
//            |   A|  YY|2001|   5| 4.0|
//            |   A|  YY|2002|  10| 2.0|
//            |   A|  YY|2003|   5| 7.0|
//            +----+----+----+----+----+
//        * */
//        ScriptContext context = engine.getContext();
//        context.setAttribute("ds1", anCountDS1, ScriptContext.ENGINE_SCOPE);
//
//
//        engine.eval("res := sum ( ds1 over ( partition by Id_1 order by Id_2 data points between 2 preceding and 2 following) );");
//        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);
//
//        /*
//        * The result data frame
//        *
//        *   +----+----+----+----+----+----------+----------+
//            |Id_1|Id_2|Year|Me_1|Me_2|sum_Me_1|sum_Me_2|
//            +----+----+----+----+----+--------+--------+
//            |   A|  XX|2000|   3| 1.0|      14|    15.0|
//            |   A|  XX|2001|   4| 9.0|      20|    23.0|
//            |   A|  XX|2002|   7| 5.0|      29|    26.0|
//            |   A|  XX|2003|   6| 8.0|      31|    29.0|
//            |   A|  YY|2000|   9| 3.0|      37|    22.0|
//            |   A|  YY|2001|   5| 4.0|      35|    24.0|
//            |   A|  YY|2002|  10| 2.0|      29|    16.0|
//            |   A|  YY|2003|   5| 7.0|      20|    13.0|
//            +----+----+----+----+----+--------+--------+
//
//        * */
//        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 14L, "Me_2", 15.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 20L, "Me_2", 23.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 29L, "Me_2", 26.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 31L, "Me_2", 29.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 37L, "Me_2", 22.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 35L, "Me_2", 24.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 29L, "Me_2", 16.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 20L, "Me_2", 13.0D)
//        );
//
//    }
//
//    @Test
//    public void testAnSumWithPartitionOrderByRangeClause() throws ScriptException {
//
//        // Analytical function count test case 5 : sum on window with partition, orderBy and range
//        /* Input dataset
//        *   +----+----+----+----+----+
//            |Id_1|Id_2|Year|Me_1|Me_2|
//            +----+----+----+----+----+
//            |   A|  XX|2000|   3| 1.0|
//            |   A|  XX|2001|   4| 9.0|
//            |   A|  XX|2002|   7| 5.0|
//            |   A|  XX|2003|   6| 8.0|
//            |   A|  YY|2000|   9| 3.0|
//            |   A|  YY|2001|   5| 4.0|
//            |   A|  YY|2002|  10| 2.0|
//            |   A|  YY|2003|   5| 7.0|
//            +----+----+----+----+----+
//        * */
//        ScriptContext context = engine.getContext();
//        context.setAttribute("ds1", anCountDS1, ScriptContext.ENGINE_SCOPE);
//
//
//        engine.eval("res := sum ( ds1 over ( partition by Id_1 order by Year range between -1 and 1) );");
//        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);
//
//        /*
//        * The result data frame
//        *
//        *   +----+----+----+----+----+----------+----------+
//            |Id_1|Id_2|Year|Me_1|Me_2|sum_Me_1|sum_Me_2|
//            +----+----+----+----+----+--------+--------+
//            |   A|  XX|2000|   3| 1.0|      21|    17.0|
//            |   A|  YY|2000|   9| 3.0|      21|    17.0|
//            |   A|  XX|2001|   4| 9.0|      38|    24.0|
//            |   A|  YY|2001|   5| 4.0|      38|    24.0|
//            |   A|  XX|2002|   7| 5.0|      37|    35.0|
//            |   A|  YY|2002|  10| 2.0|      37|    35.0|
//            |   A|  XX|2003|   6| 8.0|      28|    22.0|
//            |   A|  YY|2003|   5| 7.0|      28|    22.0|
//            +----+----+----+----+----+--------+--------+
//
//        * */
//        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 21L, "Me_2", 17.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 21L, "Me_2", 17.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 38L, "Me_2", 24.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 38L, "Me_2", 24.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 37L, "Me_2", 35.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 37L, "Me_2", 35.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 28L, "Me_2", 22.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 28L, "Me_2", 22.0D)
//        );
//
//    }
//
//    /*
//     * End of sum test case */
//
//
//    /*
//     * Test case for analytic function Min
//     *
//     * */
//
//    @Test
//    public void testAnMinWithPartitionClause() throws ScriptException {
//
//        // Analytical function Test case 1 : min on window with partition
//        /* Input dataset
//        *   +----+----+----+----+----+
//            |Id_1|Id_2|Year|Me_1|Me_2|
//            +----+----+----+----+----+
//            |   A|  XX|2000|   3| 1.0|
//            |   A|  XX|2001|   4| 9.0|
//            |   A|  XX|2002|   7| 5.0|
//            |   A|  XX|2003|   6| 8.0|
//            |   A|  YY|2000|   9| 3.0|
//            |   A|  YY|2001|   5| 4.0|
//            |   A|  YY|2002|  10| 2.0|
//            |   A|  YY|2003|   5| 7.0|
//            +----+----+----+----+----+
//        * */
//        ScriptContext context = engine.getContext();
//        context.setAttribute("ds1", anCountDS1, ScriptContext.ENGINE_SCOPE);
//
//
//        engine.eval("res := min ( ds1 over ( partition by Id_1, Id_2 ) );");
//        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);
//
//        /*
//        *
//        *   +----+----+----+----+----+----------+----------+
//            |Id_1|Id_2|Year|Me_1|Me_2|min_Me_1|min_Me_2|
//            +----+----+----+----+----+--------+--------+
//            |   A|  XX|2000|   3| 1.0|       3|     1.0|
//            |   A|  XX|2001|   4| 9.0|       3|     1.0|
//            |   A|  XX|2002|   7| 5.0|       3|     1.0|
//            |   A|  XX|2003|   6| 8.0|       3|     1.0|
//            |   A|  YY|2000|   9| 3.0|       5|     2.0|
//            |   A|  YY|2001|   5| 4.0|       5|     2.0|
//            |   A|  YY|2002|  10| 2.0|       5|     2.0|
//            |   A|  YY|2003|   5| 7.0|       5|     2.0|
//            +----+----+----+----+----+--------+--------+
//        * */
//        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 3L, "Me_2", 1.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 3L, "Me_2", 1.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 3L, "Me_2", 1.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 3L, "Me_2", 1.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 5L, "Me_2", 2.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 5L, "Me_2", 2.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 5L, "Me_2", 2.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 5L, "Me_2", 2.0D)
//        );
//
//    }
//
//    @Test
//    public void testAnMinWithPartitionOrderByClause() throws ScriptException {
//
//        // Analytical function Test case 2 : min on window with partition and order by
//        /* Input dataset
//        *   +----+----+----+----+----+
//            |Id_1|Id_2|Year|Me_1|Me_2|
//            +----+----+----+----+----+
//            |   A|  XX|2000|   3| 1.0|
//            |   A|  XX|2001|   4| 9.0|
//            |   A|  XX|2002|   7| 5.0|
//            |   A|  XX|2003|   6| 8.0|
//            |   A|  YY|2000|   9| 3.0|
//            |   A|  YY|2001|   5| 4.0|
//            |   A|  YY|2002|  10| 2.0|
//            |   A|  YY|2003|   5| 7.0|
//            +----+----+----+----+----+
//        * */
//        ScriptContext context = engine.getContext();
//        context.setAttribute("ds1", anCountDS1, ScriptContext.ENGINE_SCOPE);
//
//
//        engine.eval("res := min ( ds1 over ( partition by Id_1, Id_2 order by Year) );");
//        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);
//
//        /*
//        *   +----+----+----+----+----+----------+----------+
//            |Id_1|Id_2|Year|Me_1|Me_2|min_Me_1|min_Me_2|
//            +----+----+----+----+----+--------+--------+
//            |   A|  XX|2000|   3| 1.0|       3|     1.0|
//            |   A|  XX|2001|   4| 9.0|       3|     1.0|
//            |   A|  XX|2002|   7| 5.0|       3|     1.0|
//            |   A|  XX|2003|   6| 8.0|       3|     1.0|
//            |   A|  YY|2000|   9| 3.0|       9|     3.0|
//            |   A|  YY|2001|   5| 4.0|       5|     3.0|
//            |   A|  YY|2002|  10| 2.0|       5|     2.0|
//            |   A|  YY|2003|   5| 7.0|       5|     2.0|
//            +----+----+----+----+----+--------+--------+
//
//        * */
//        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 3L, "Me_2", 1.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 3L, "Me_2", 1.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 3L, "Me_2", 1.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 3L, "Me_2", 1.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 9L, "Me_2", 3.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 5L, "Me_2", 3.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 5L, "Me_2", 2.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 5L, "Me_2", 2.0D)
//        );
//
//    }
//
//    @Test
//    public void testAnMinWithOrderByClause() throws ScriptException {
//
//        // Analytical function Test case 3 : min on window with only order by without partition
//        /* Input dataset
//        *   +----+----+----+----+----+
//            |Id_1|Id_2|Year|Me_1|Me_2|
//            +----+----+----+----+----+
//            |   A|  XX|2000|   3| 1.0|
//            |   A|  XX|2001|   4| 9.0|
//            |   A|  XX|2002|   7| 5.0|
//            |   A|  XX|2003|   6| 8.0|
//            |   A|  YY|2000|   9| 3.0|
//            |   A|  YY|2001|   5| 4.0|
//            |   A|  YY|2002|  10| 2.0|
//            |   A|  YY|2003|   5| 7.0|
//            +----+----+----+----+----+
//        * */
//        ScriptContext context = engine.getContext();
//        context.setAttribute("ds1", anCountDS1, ScriptContext.ENGINE_SCOPE);
//
//
//        engine.eval("res := min ( ds1 over ( order by Id_1, Id_2, Year ) );");
//        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);
//
//        /*
//        *
//        *   +----+----+----+----+----+----------+----------+
//            |Id_1|Id_2|Year|Me_1|Me_2|min_Me_1|min_Me_2|
//            +----+----+----+----+----+--------+--------+
//            |   A|  XX|2000|   3| 1.0|       3|     1.0|
//            |   A|  XX|2001|   4| 9.0|       3|     1.0|
//            |   A|  XX|2002|   7| 5.0|       3|     1.0|
//            |   A|  XX|2003|   6| 8.0|       3|     1.0|
//            |   A|  YY|2000|   9| 3.0|       3|     1.0|
//            |   A|  YY|2001|   5| 4.0|       3|     1.0|
//            |   A|  YY|2002|  10| 2.0|       3|     1.0|
//            |   A|  YY|2003|   5| 7.0|       3|     1.0|
//            +----+----+----+----+----+--------+--------+
//
//        * */
//        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 3L, "Me_2", 1.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 3L, "Me_2", 1.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 3L, "Me_2", 1.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 3L, "Me_2", 1.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 3L, "Me_2", 1.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 3L, "Me_2", 1.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 3L, "Me_2", 1.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 3L, "Me_2", 1.0D)
//        );
//
//    }
//
//    @Test
//    public void testAnMinWithPartitionOrderByDPClause() throws ScriptException {
//
//        // Analytical function count test case 4 : min on window with partition, orderBy and data points
//        /* Input dataset
//        *   +----+----+----+----+----+
//            |Id_1|Id_2|Year|Me_1|Me_2|
//            +----+----+----+----+----+
//            |   A|  XX|2000|   3| 1.0|
//            |   A|  XX|2001|   4| 9.0|
//            |   A|  XX|2002|   7| 5.0|
//            |   A|  XX|2003|   6| 8.0|
//            |   A|  YY|2000|   9| 3.0|
//            |   A|  YY|2001|   5| 4.0|
//            |   A|  YY|2002|  10| 2.0|
//            |   A|  YY|2003|   5| 7.0|
//            +----+----+----+----+----+
//        * */
//        ScriptContext context = engine.getContext();
//        context.setAttribute("ds1", anCountDS1, ScriptContext.ENGINE_SCOPE);
//
//        engine.eval("res := min ( ds1 over ( partition by Id_1 order by Id_2 data points between 2 preceding and 2 following) );");
//        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);
//
//        /*
//        * The result data frame
//        *
//        *   +----+----+----+----+----+----------+----------+
//            |Id_1|Id_2|Year|Me_1|Me_2|min_Me_1|min_Me_2|
//            +----+----+----+----+----+--------+--------+
//            |   A|  XX|2000|   3| 1.0|       3|     1.0|
//            |   A|  XX|2001|   4| 9.0|       3|     1.0|
//            |   A|  XX|2002|   7| 5.0|       3|     1.0|
//            |   A|  XX|2003|   6| 8.0|       4|     3.0|
//            |   A|  YY|2000|   9| 3.0|       5|     2.0|
//            |   A|  YY|2001|   5| 4.0|       5|     2.0|
//            |   A|  YY|2002|  10| 2.0|       5|     2.0|
//            |   A|  YY|2003|   5| 7.0|       5|     2.0|
//            +----+----+----+----+----+--------+--------+
//
//        * */
//        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 3L, "Me_2", 1.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 3L, "Me_2", 1.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 3L, "Me_2", 1.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 4L, "Me_2", 3.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 5L, "Me_2", 2.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 5L, "Me_2", 2.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 5L, "Me_2", 2.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 5L, "Me_2", 2.0D)
//        );
//
//    }
//
//    @Test
//    public void testAnMinWithPartitionOrderByRangeClause() throws ScriptException {
//
//        // Analytical function count test case 5 : min on window with partition, orderBy and range
//        /* Input dataset
//        *   +----+----+----+----+----+
//            |Id_1|Id_2|Year|Me_1|Me_2|
//            +----+----+----+----+----+
//            |   A|  XX|2000|   3| 1.0|
//            |   A|  XX|2001|   4| 9.0|
//            |   A|  XX|2002|   7| 5.0|
//            |   A|  XX|2003|   6| 8.0|
//            |   A|  YY|2000|   9| 3.0|
//            |   A|  YY|2001|   5| 4.0|
//            |   A|  YY|2002|  10| 2.0|
//            |   A|  YY|2003|   5| 7.0|
//            +----+----+----+----+----+
//        * */
//        ScriptContext context = engine.getContext();
//        context.setAttribute("ds1", anCountDS1, ScriptContext.ENGINE_SCOPE);
//
//
//        engine.eval("res := min ( ds1 over ( partition by Id_1 order by Year range between -1 and 1) );");
//        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);
//
//        /*
//        * The result data frame
//        *
//        *   +----+----+----+----+----+----------+----------+
//            |Id_1|Id_2|Year|Me_1|Me_2|min_Me_1|min_Me_2|
//            +----+----+----+----+----+--------+--------+
//            |   A|  XX|2000|   3| 1.0|       3|     1.0|
//            |   A|  YY|2000|   9| 3.0|       3|     1.0|
//            |   A|  XX|2001|   4| 9.0|       3|     1.0|
//            |   A|  YY|2001|   5| 4.0|       3|     1.0|
//            |   A|  XX|2002|   7| 5.0|       4|     2.0|
//            |   A|  YY|2002|  10| 2.0|       4|     2.0|
//            |   A|  XX|2003|   6| 8.0|       5|     2.0|
//            |   A|  YY|2003|   5| 7.0|       5|     2.0|
//            +----+----+----+----+----+--------+--------+
//
//        * */
//        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 3L, "Me_2", 1.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 3L, "Me_2", 1.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 3L, "Me_2", 1.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 3L, "Me_2", 1.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 4L, "Me_2", 2.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 4L, "Me_2", 2.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 5L, "Me_2", 2.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 5L, "Me_2", 2.0D)
//        );
//
//    }
//
//    /*
//     * End of Min test case */
//
//
//    /*
//     * Test case for analytic function Max
//     *
//     * */
//    @Test
//    public void testAnMaxWithPartitionClause() throws ScriptException {
//
//        // Analytical function Test case 1 : max on window with partition
//        /* Input dataset
//        *   +----+----+----+----+----+
//            |Id_1|Id_2|Year|Me_1|Me_2|
//            +----+----+----+----+----+
//            |   A|  XX|2000|   3| 1.0|
//            |   A|  XX|2001|   4| 9.0|
//            |   A|  XX|2002|   7| 5.0|
//            |   A|  XX|2003|   6| 8.0|
//            |   A|  YY|2000|   9| 3.0|
//            |   A|  YY|2001|   5| 4.0|
//            |   A|  YY|2002|  10| 2.0|
//            |   A|  YY|2003|   5| 7.0|
//            +----+----+----+----+----+
//        * */
//        ScriptContext context = engine.getContext();
//        context.setAttribute("ds1", anCountDS1, ScriptContext.ENGINE_SCOPE);
//
//
//        engine.eval("res := max ( ds1 over ( partition by Id_1, Id_2 ) );");
//        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);
//
//        /*
//        *
//        *   +----+----+----+----+----+----------+----------+
//            |Id_1|Id_2|Year|Me_1|Me_2|max_Me_1|max_Me_2|
//            +----+----+----+----+----+--------+--------+
//            |   A|  XX|2000|   3| 1.0|       7|     9.0|
//            |   A|  XX|2001|   4| 9.0|       7|     9.0|
//            |   A|  XX|2002|   7| 5.0|       7|     9.0|
//            |   A|  XX|2003|   6| 8.0|       7|     9.0|
//            |   A|  YY|2000|   9| 3.0|      10|     7.0|
//            |   A|  YY|2001|   5| 4.0|      10|     7.0|
//            |   A|  YY|2002|  10| 2.0|      10|     7.0|
//            |   A|  YY|2003|   5| 7.0|      10|     7.0|
//            +----+----+----+----+----+--------+--------+
//        * */
//        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 7L, "Me_2", 9.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 7L, "Me_2", 9.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 7L, "Me_2", 9.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 7L, "Me_2", 9.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 10L, "Me_2", 7.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 10L, "Me_2", 7.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 10L, "Me_2", 7.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 10L, "Me_2", 7.0D)
//        );
//
//    }
//
//    @Test
//    public void testAnMaxWithPartitionOrderByClause() throws ScriptException {
//
//        // Analytical function Test case 2 : max on window with partition and order by
//        /* Input dataset
//        *   +----+----+----+----+----+
//            |Id_1|Id_2|Year|Me_1|Me_2|
//            +----+----+----+----+----+
//            |   A|  XX|2000|   3| 1.0|
//            |   A|  XX|2001|   4| 9.0|
//            |   A|  XX|2002|   7| 5.0|
//            |   A|  XX|2003|   6| 8.0|
//            |   A|  YY|2000|   9| 3.0|
//            |   A|  YY|2001|   5| 4.0|
//            |   A|  YY|2002|  10| 2.0|
//            |   A|  YY|2003|   5| 7.0|
//            +----+----+----+----+----+
//        * */
//        ScriptContext context = engine.getContext();
//        context.setAttribute("ds1", anCountDS1, ScriptContext.ENGINE_SCOPE);
//
//
//        engine.eval("res := max ( ds1 over ( partition by Id_1, Id_2 order by Year) );");
//        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);
//
//        /*
//        *   +----+----+----+----+----+----------+----------+
//            |Id_1|Id_2|Year|Me_1|Me_2|max_Me_1|max_Me_2|
//            +----+----+----+----+----+--------+--------+
//            |   A|  XX|2000|   3| 1.0|       3|     1.0|
//            |   A|  XX|2001|   4| 9.0|       4|     9.0|
//            |   A|  XX|2002|   7| 5.0|       7|     9.0|
//            |   A|  XX|2003|   6| 8.0|       7|     9.0|
//            |   A|  YY|2000|   9| 3.0|       9|     3.0|
//            |   A|  YY|2001|   5| 4.0|       9|     4.0|
//            |   A|  YY|2002|  10| 2.0|      10|     4.0|
//            |   A|  YY|2003|   5| 7.0|      10|     7.0|
//            +----+----+----+----+----+--------+--------+
//
//        * */
//        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 3L, "Me_2", 1.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 4L, "Me_2", 9.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 7L, "Me_2", 9.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 7L, "Me_2", 9.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 9L, "Me_2", 3.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 9L, "Me_2", 4.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 10L, "Me_2", 4.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 10L, "Me_2", 7.0D)
//        );
//
//    }
//
//
//    @Test
//    public void testAnMaxWithPartitionOrderByDPClause() throws ScriptException {
//
//        // Analytical function count test case 3 : max on window with partition, orderBy and data points
//        /* Input dataset
//        *   +----+----+----+----+----+
//            |Id_1|Id_2|Year|Me_1|Me_2|
//            +----+----+----+----+----+
//            |   A|  XX|2000|   3| 1.0|
//            |   A|  XX|2001|   4| 9.0|
//            |   A|  XX|2002|   7| 5.0|
//            |   A|  XX|2003|   6| 8.0|
//            |   A|  YY|2000|   9| 3.0|
//            |   A|  YY|2001|   5| 4.0|
//            |   A|  YY|2002|  10| 2.0|
//            |   A|  YY|2003|   5| 7.0|
//            +----+----+----+----+----+
//        * */
//        ScriptContext context = engine.getContext();
//        context.setAttribute("ds1", anCountDS1, ScriptContext.ENGINE_SCOPE);
//
//        engine.eval("res := max ( ds1 over ( partition by Id_1 order by Id_2 data points between 2 preceding and 2 following) );");
//        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);
//
//        /*
//        * The result data frame
//        *
//        *   +----+----+----+----+----+----------+----------+
//            |Id_1|Id_2|Year|Me_1|Me_2|max_Me_1|max_Me_2|
//            +----+----+----+----+----+--------+--------+
//            |   A|  XX|2000|   3| 1.0|       7|     9.0|
//            |   A|  XX|2001|   4| 9.0|       7|     9.0|
//            |   A|  XX|2002|   7| 5.0|       9|     9.0|
//            |   A|  XX|2003|   6| 8.0|       9|     9.0|
//            |   A|  YY|2000|   9| 3.0|      10|     8.0|
//            |   A|  YY|2001|   5| 4.0|      10|     8.0|
//            |   A|  YY|2002|  10| 2.0|      10|     7.0|
//            |   A|  YY|2003|   5| 7.0|      10|     7.0|
//            +----+----+----+----+----+--------+--------+
//
//        * */
//        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 7L, "Me_2", 9.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 7L, "Me_2", 9.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 9L, "Me_2", 9.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 9L, "Me_2", 9.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 10L, "Me_2", 8.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 10L, "Me_2", 8.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 10L, "Me_2", 7.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 10L, "Me_2", 7.0D)
//        );
//
//    }
//
//    @Test
//    public void testAnMaxWithPartitionOrderByRangeClause() throws ScriptException {
//
//        // Analytical function count test case 5 : min on window with partition, orderBy and range
//        /* Input dataset
//        *   +----+----+----+----+----+
//            |Id_1|Id_2|Year|Me_1|Me_2|
//            +----+----+----+----+----+
//            |   A|  XX|2000|   3| 1.0|
//            |   A|  XX|2001|   4| 9.0|
//            |   A|  XX|2002|   7| 5.0|
//            |   A|  XX|2003|   6| 8.0|
//            |   A|  YY|2000|   9| 3.0|
//            |   A|  YY|2001|   5| 4.0|
//            |   A|  YY|2002|  10| 2.0|
//            |   A|  YY|2003|   5| 7.0|
//            +----+----+----+----+----+
//        * */
//        ScriptContext context = engine.getContext();
//        context.setAttribute("ds1", anCountDS1, ScriptContext.ENGINE_SCOPE);
//
//
//        engine.eval("res := min ( ds1 over ( partition by Id_1 order by Year range between -1 and 1) );");
//        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);
//
//        /*
//        * The result data frame
//        *
//        *   +----+----+----+----+----+----------+----------+
//            |Id_1|Id_2|Year|Me_1|Me_2|max_Me_1|max_Me_2|
//            +----+----+----+----+----+--------+--------+
//            |   A|  XX|2000|   3| 1.0|       9|     9.0|
//            |   A|  YY|2000|   9| 3.0|       9|     9.0|
//            |   A|  XX|2001|   4| 9.0|      10|     9.0|
//            |   A|  YY|2001|   5| 4.0|      10|     9.0|
//            |   A|  XX|2002|   7| 5.0|      10|     9.0|
//            |   A|  YY|2002|  10| 2.0|      10|     9.0|
//            |   A|  XX|2003|   6| 8.0|      10|     8.0|
//            |   A|  YY|2003|   5| 7.0|      10|     8.0|
//            +----+----+----+----+----+--------+--------+
//
//        * */
//        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 9L, "Me_2", 9.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 9L, "Me_2", 9.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 10L, "Me_2", 9.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 10L, "Me_2", 9.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 10L, "Me_2", 9.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 10L, "Me_2", 9.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 10L, "Me_2", 8.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 10L, "Me_2", 8.0D)
//        );
//
//    }
//    /*
//     * End of Max test case */
//
//    /*
//     * Test case for analytic function Avg
//     *
//     * */
//    @Test
//    public void testAnAvgWithPartitionClause() throws ScriptException {
//
//        // Analytical function Test case 1 : avg on window with partition
//        /* Input dataset
//        *   +----+----+----+----+----+
//            |Id_1|Id_2|Year|Me_1|Me_2|
//            +----+----+----+----+----+
//            |   A|  XX|2000|   3| 1.0|
//            |   A|  XX|2001|   4| 9.0|
//            |   A|  XX|2002|   7| 5.0|
//            |   A|  XX|2003|   6| 8.0|
//            |   A|  YY|2000|   9| 3.0|
//            |   A|  YY|2001|   5| 4.0|
//            |   A|  YY|2002|  10| 2.0|
//            |   A|  YY|2003|   5| 7.0|
//            +----+----+----+----+----+
//        * */
//        ScriptContext context = engine.getContext();
//        context.setAttribute("ds1", anCountDS1, ScriptContext.ENGINE_SCOPE);
//
//
//        engine.eval("res := avg ( ds1 over ( partition by Id_1, Id_2 ) );");
//        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);
//
//        /*
//        *
//        *   +----+----+----+----+----+----------+----------+
//            |Id_1|Id_2|Year|Me_1|Me_2|avg_Me_1|avg_Me_2|
//            +----+----+----+----+----+--------+--------+
//            |   A|  XX|2000|   3| 1.0|     5.0|    5.75|
//            |   A|  XX|2001|   4| 9.0|     5.0|    5.75|
//            |   A|  XX|2002|   7| 5.0|     5.0|    5.75|
//            |   A|  XX|2003|   6| 8.0|     5.0|    5.75|
//            |   A|  YY|2000|   9| 3.0|    7.25|     4.0|
//            |   A|  YY|2001|   5| 4.0|    7.25|     4.0|
//            |   A|  YY|2002|  10| 2.0|    7.25|     4.0|
//            |   A|  YY|2003|   5| 7.0|    7.25|     4.0|
//            +----+----+----+----+----+--------+--------+
//        * */
//        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 5.0D, "Me_2", 5.75D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 5.0D, "Me_2", 5.75D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 5.0D, "Me_2", 5.75D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 5.0D, "Me_2", 5.75D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 7.25D, "Me_2", 4.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 7.25D, "Me_2", 4.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 7.25D, "Me_2", 4.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 7.25D, "Me_2", 4.0D)
//        );
//
//    }
//
//    @Test
//    public void testAnAvgWithPartitionOrderByClause() throws ScriptException {
//
//        // Analytical function Test case 2 : avg on window with partition and order by
//        /* Input dataset
//        *   +----+----+----+----+----+
//            |Id_1|Id_2|Year|Me_1|Me_2|
//            +----+----+----+----+----+
//            |   A|  XX|2000|   3| 1.0|
//            |   A|  XX|2001|   4| 9.0|
//            |   A|  XX|2002|   7| 5.0|
//            |   A|  XX|2003|   6| 8.0|
//            |   A|  YY|2000|   9| 3.0|
//            |   A|  YY|2001|   5| 4.0|
//            |   A|  YY|2002|  10| 2.0|
//            |   A|  YY|2003|   5| 7.0|
//            +----+----+----+----+----+
//        * */
//        ScriptContext context = engine.getContext();
//        context.setAttribute("ds1", anCountDS1, ScriptContext.ENGINE_SCOPE);
//
//
//        engine.eval("res := avg ( ds1 over ( partition by Id_1, Id_2 order by Year) );");
//        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);
//
//        /*
//        *   +----+----+----+----+----+----------+----------+
//            |Id_1|Id_2|Year|Me_1|Me_2|         avg_Me_1|avg_Me_2|
//            +----+----+----+----+----+-----------------+--------+
//            |   A|  XX|2000|   3| 1.0|              3.0|     1.0|
//            |   A|  XX|2001|   4| 9.0|              3.5|     5.0|
//            |   A|  XX|2002|   7| 5.0|4.666666666666667|     5.0|
//            |   A|  XX|2003|   6| 8.0|              5.0|    5.75|
//            |   A|  YY|2000|   9| 3.0|              9.0|     3.0|
//            |   A|  YY|2001|   5| 4.0|              7.0|     3.5|
//            |   A|  YY|2002|  10| 2.0|              8.0|     3.0|
//            |   A|  YY|2003|   5| 7.0|             7.25|     4.0|
//            +----+----+----+----+----+-----------------+--------+
//
//        * */
//        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 3.0D, "Me_2", 1.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 3.5D, "Me_2", 5.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 4.67D, "Me_2", 5.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 5.0D, "Me_2", 5.75D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 9.0D, "Me_2", 3.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 7.0D, "Me_2", 3.5D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 8.0D, "Me_2", 3.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 7.25D, "Me_2", 4.0D)
//        );
//
//    }
//
//
//    @Test
//    public void testAnAvgWithPartitionOrderByDPClause() throws ScriptException {
//
//        // Analytical function count test case 3 : avg on window with partition, orderBy and data points
//        /* Input dataset
//        *   +----+----+----+----+----+
//            |Id_1|Id_2|Year|Me_1|Me_2|
//            +----+----+----+----+----+
//            |   A|  XX|2000|   3| 1.0|
//            |   A|  XX|2001|   4| 9.0|
//            |   A|  XX|2002|   7| 5.0|
//            |   A|  XX|2003|   6| 8.0|
//            |   A|  YY|2000|   9| 3.0|
//            |   A|  YY|2001|   5| 4.0|
//            |   A|  YY|2002|  10| 2.0|
//            |   A|  YY|2003|   5| 7.0|
//            +----+----+----+----+----+
//        * */
//        ScriptContext context = engine.getContext();
//        context.setAttribute("ds1", anCountDS1, ScriptContext.ENGINE_SCOPE);
//
//        engine.eval("res := avg ( ds1 over ( partition by Id_1 order by Id_2 data points between 2 preceding and 2 following) );");
//        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);
//
//        /*
//        * The result data frame
//        *
//        *   +----+----+----+----+----+----------+----------+
//            |Id_1|Id_2|Year|Me_1|Me_2|         avg_Me_1|         avg_Me_2|
//            +----+----+----+----+----+-----------------+-----------------+
//            |   A|  XX|2000|   3| 1.0|4.666666666666667|              5.0|
//            |   A|  XX|2001|   4| 9.0|              5.0|             5.75|
//            |   A|  XX|2002|   7| 5.0|              5.8|              5.2|
//            |   A|  XX|2003|   6| 8.0|              6.2|              5.8|
//            |   A|  YY|2000|   9| 3.0|              7.4|              4.4|
//            |   A|  YY|2001|   5| 4.0|              7.0|              4.8|
//            |   A|  YY|2002|  10| 2.0|             7.25|              4.0|
//            |   A|  YY|2003|   5| 7.0|6.666666666666667|4.333333333333333|
//            +----+----+----+----+----+-----------------+-----------------+
//
//        * */
//        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 4.67D, "Me_2", 5.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 5.0D, "Me_2", 5.75D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 5.8D, "Me_2", 5.2D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 6.2D, "Me_2", 5.8D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 7.4D, "Me_2", 4.4D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 7.0D, "Me_2", 4.8D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 7.25D, "Me_2", 4.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 6.67D, "Me_2", 4.33D)
//        );
//
//    }
//
//    @Test
//    public void testAnAvgWithPartitionOrderByRangeClause() throws ScriptException {
//
//        // Analytical function count test case 5 : avg on window with partition, orderBy and range
//        /* Input dataset
//        *   +----+----+----+----+----+
//            |Id_1|Id_2|Year|Me_1|Me_2|
//            +----+----+----+----+----+
//            |   A|  XX|2000|   3| 1.0|
//            |   A|  XX|2001|   4| 9.0|
//            |   A|  XX|2002|   7| 5.0|
//            |   A|  XX|2003|   6| 8.0|
//            |   A|  YY|2000|   9| 3.0|
//            |   A|  YY|2001|   5| 4.0|
//            |   A|  YY|2002|  10| 2.0|
//            |   A|  YY|2003|   5| 7.0|
//            +----+----+----+----+----+
//        * */
//        ScriptContext context = engine.getContext();
//        context.setAttribute("ds1", anCountDS1, ScriptContext.ENGINE_SCOPE);
//
//
//        engine.eval("res := avg ( ds1 over ( partition by Id_1 order by Year range between -1 and 1) );");
//        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);
//
//        /*
//        * The result data frame
//        *
//        *   +----+----+----+----+----+----------+----------+
//            |Id_1|Id_2|Year|Me_1|Me_2|         avg_Me_1|         avg_Me_2|
//            +----+----+----+----+----+-----------------+-----------------+
//            |   A|  XX|2000|   3| 1.0|             5.25|             4.25|
//            |   A|  YY|2000|   9| 3.0|             5.25|             4.25|
//            |   A|  XX|2001|   4| 9.0|6.333333333333333|              4.0|
//            |   A|  YY|2001|   5| 4.0|6.333333333333333|              4.0|
//            |   A|  XX|2002|   7| 5.0|6.166666666666667|5.833333333333333|
//            |   A|  YY|2002|  10| 2.0|6.166666666666667|5.833333333333333|
//            |   A|  XX|2003|   6| 8.0|              7.0|              5.5|
//            |   A|  YY|2003|   5| 7.0|              7.0|              5.5|
//            +----+----+----+----+----+-----------------+-----------------+
//
//        * */
//        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 5.25D, "Me_2", 4.25D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 5.25D, "Me_2", 4.25D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 6.33D, "Me_2", 4.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 6.33D, "Me_2", 4.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 6.17D, "Me_2", 5.83D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 6.17D, "Me_2", 5.83D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 7.0D, "Me_2", 5.5D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 7.0D, "Me_2", 5.5D)
//        );
//
//    }
//    /*
//     * End of Avg test case */
//
//    /*
//     * Test case for analytic function Median
//     *
//     * */
//
//    @Test
//    public void testAnMedianWithPartitionClause() throws ScriptException {
//
//        // Analytical function Test case 1 : median on window with partition
//        /* Input dataset
//        *   +----+----+----+----+----+
//            |Id_1|Id_2|Year|Me_1|Me_2|
//            +----+----+----+----+----+
//            |   A|  XX|2000|   3| 1.0|
//            |   A|  XX|2001|   4| 9.0|
//            |   A|  XX|2002|   7| 5.0|
//            |   A|  XX|2003|   6| 8.0|
//            |   A|  YY|2000|   9| 3.0|
//            |   A|  YY|2001|   5| 4.0|
//            |   A|  YY|2002|  10| 2.0|
//            |   A|  YY|2003|   5| 7.0|
//            +----+----+----+----+----+
//        * */
//        ScriptContext context = engine.getContext();
//        context.setAttribute("ds1", anCountDS1, ScriptContext.ENGINE_SCOPE);
//
//
//        engine.eval("res := median ( ds1 over ( partition by Id_1, Id_2 ) );");
//        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);
//
//        /*
//        *
//        *   +----+----+----+----+----+----------+----------+
//            |Id_1|Id_2|Year|Me_1|Me_2|median_Me_1|median_Me_2|
//            +----+----+----+----+----+-----------+-----------+
//            |   A|  XX|2000|   3| 1.0|          4|        5.0|
//            |   A|  XX|2001|   4| 9.0|          4|        5.0|
//            |   A|  XX|2002|   7| 5.0|          4|        5.0|
//            |   A|  XX|2003|   6| 8.0|          4|        5.0|
//            |   A|  YY|2000|   9| 3.0|          5|        3.0|
//            |   A|  YY|2001|   5| 4.0|          5|        3.0|
//            |   A|  YY|2002|  10| 2.0|          5|        3.0|
//            |   A|  YY|2003|   5| 7.0|          5|        3.0|
//            +----+----+----+----+----+-----------+-----------+
//        * */
//        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 4L, "Me_2", 5.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 4L, "Me_2", 5.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 4L, "Me_2", 5.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 4L, "Me_2", 5.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 5L, "Me_2", 3.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 5L, "Me_2", 3.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 5L, "Me_2", 3.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 5L, "Me_2", 3.0D)
//        );
//
//    }
//
//    @Test
//    public void testAnMedianWithPartitionOrderByClause() throws ScriptException {
//
//        // Analytical function Test case 2 : median on window with partition and order by
//        /* Input dataset
//        *   +----+----+----+----+----+
//            |Id_1|Id_2|Year|Me_1|Me_2|
//            +----+----+----+----+----+
//            |   A|  XX|2000|   3| 1.0|
//            |   A|  XX|2001|   4| 9.0|
//            |   A|  XX|2002|   7| 5.0|
//            |   A|  XX|2003|   6| 8.0|
//            |   A|  YY|2000|   9| 3.0|
//            |   A|  YY|2001|   5| 4.0|
//            |   A|  YY|2002|  10| 2.0|
//            |   A|  YY|2003|   5| 7.0|
//            +----+----+----+----+----+
//        * */
//        ScriptContext context = engine.getContext();
//        context.setAttribute("ds1", anCountDS1, ScriptContext.ENGINE_SCOPE);
//
//
//        engine.eval("res := median ( ds1 over ( partition by Id_1, Id_2 order by Year) );");
//        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);
//
//        /*
//        *   +----+----+----+----+----+----------+----------+
//            |Id_1|Id_2|Year|Me_1|Me_2|median_Me_1|median_Me_2|
//            +----+----+----+----+----+-----------+-----------+
//            |   A|  XX|2000|   3| 1.0|          3|        1.0|
//            |   A|  XX|2001|   4| 9.0|          3|        1.0|
//            |   A|  XX|2002|   7| 5.0|          4|        5.0|
//            |   A|  XX|2003|   6| 8.0|          4|        5.0|
//            |   A|  YY|2000|   9| 3.0|          9|        3.0|
//            |   A|  YY|2001|   5| 4.0|          5|        3.0|
//            |   A|  YY|2002|  10| 2.0|          9|        3.0|
//            |   A|  YY|2003|   5| 7.0|          5|        3.0|
//            +----+----+----+----+----+-----------+-----------+
//
//        * */
//        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 3L, "Me_2", 1.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 3L, "Me_2", 1.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 4L, "Me_2", 5.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 4L, "Me_2", 5.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 9L, "Me_2", 3.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 5L, "Me_2", 3.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 9L, "Me_2", 3.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 5L, "Me_2", 3.0D)
//        );
//
//    }
//
//
//    @Test
//    public void testAnMedianWithPartitionOrderByDPClause() throws ScriptException {
//
//        // Analytical function count test case 3 : median on window with partition, orderBy and data points
//        /* Input dataset
//        *   +----+----+----+----+----+
//            |Id_1|Id_2|Year|Me_1|Me_2|
//            +----+----+----+----+----+
//            |   A|  XX|2000|   3| 1.0|
//            |   A|  XX|2001|   4| 9.0|
//            |   A|  XX|2002|   7| 5.0|
//            |   A|  XX|2003|   6| 8.0|
//            |   A|  YY|2000|   9| 3.0|
//            |   A|  YY|2001|   5| 4.0|
//            |   A|  YY|2002|  10| 2.0|
//            |   A|  YY|2003|   5| 7.0|
//            +----+----+----+----+----+
//        * */
//        ScriptContext context = engine.getContext();
//        context.setAttribute("ds1", anCountDS1, ScriptContext.ENGINE_SCOPE);
//
//        engine.eval("res := median ( ds1 over ( partition by Id_1 order by Id_2 data points between 2 preceding and 2 following) );");
//        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);
//
//        /*
//        * The result data frame
//        *
//        *   +----+----+----+----+----+----------+----------+
//            |Id_1|Id_2|Year|Me_1|Me_2|median_Me_1|median_Me_2|
//            +----+----+----+----+----+-----------+-----------+
//            |   A|  XX|2000|   3| 1.0|          4|        5.0|
//            |   A|  XX|2001|   4| 9.0|          4|        5.0|
//            |   A|  XX|2002|   7| 5.0|          6|        5.0|
//            |   A|  XX|2003|   6| 8.0|          6|        5.0|
//            |   A|  YY|2000|   9| 3.0|          7|        4.0|
//            |   A|  YY|2001|   5| 4.0|          6|        4.0|
//            |   A|  YY|2002|  10| 2.0|          5|        3.0|
//            |   A|  YY|2003|   5| 7.0|          5|        4.0|
//            +----+----+----+----+----+-----------+-----------+
//
//        * */
//        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 4L, "Me_2", 5.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 4L, "Me_2", 5.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 6L, "Me_2", 5.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 6L, "Me_2", 5.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 7L, "Me_2", 4.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 6L, "Me_2", 4.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 5L, "Me_2", 3.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 5L, "Me_2", 4.0D)
//        );
//
//    }
//
//    @Test
//    public void testAnMedianWithPartitionOrderByRangeClause() throws ScriptException {
//
//        // Analytical function count test case 4 : median on window with partition, orderBy and range
//        /* Input dataset
//        *   +----+----+----+----+----+
//            |Id_1|Id_2|Year|Me_1|Me_2|
//            +----+----+----+----+----+
//            |   A|  XX|2000|   3| 1.0|
//            |   A|  XX|2001|   4| 9.0|
//            |   A|  XX|2002|   7| 5.0|
//            |   A|  XX|2003|   6| 8.0|
//            |   A|  YY|2000|   9| 3.0|
//            |   A|  YY|2001|   5| 4.0|
//            |   A|  YY|2002|  10| 2.0|
//            |   A|  YY|2003|   5| 7.0|
//            +----+----+----+----+----+
//        * */
//        ScriptContext context = engine.getContext();
//        context.setAttribute("ds1", anCountDS1, ScriptContext.ENGINE_SCOPE);
//
//
//        engine.eval("res := median ( ds1 over ( partition by Id_1 order by Year range between -1 and 1) );");
//        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);
//
//        /*
//        * The result data frame
//        *
//        *   +----+----+----+----+----+----------+----------+
//            |Id_1|Id_2|Year|Me_1|Me_2|median_Me_1|median_Me_2|
//            +----+----+----+----+----+-----------+-----------+
//            |   A|  XX|2000|   3| 1.0|          4|        3.0|
//            |   A|  YY|2000|   9| 3.0|          4|        3.0|
//            |   A|  XX|2001|   4| 9.0|          5|        3.0|
//            |   A|  YY|2001|   5| 4.0|          5|        3.0|
//            |   A|  XX|2002|   7| 5.0|          5|        5.0|
//            |   A|  YY|2002|  10| 2.0|          5|        5.0|
//            |   A|  XX|2003|   6| 8.0|          6|        5.0|
//            |   A|  YY|2003|   5| 7.0|          6|        5.0|
//            +----+----+----+----+----+-----------+-----------+
//
//        * */
//        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 4L, "Me_2", 3.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 4L, "Me_2", 3.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 5L, "Me_2", 3.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 5L, "Me_2", 3.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 5L, "Me_2", 5.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 5L, "Me_2", 5.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 6L, "Me_2", 5.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 6L, "Me_2", 5.0D)
//        );
//
//    }
//
//    /*
//     * End of Median test case */
//
//    /*
//     * Test case for analytic function stddev_pop
//     *
//     * */
//
//    @Test
//    public void testAnStdPopWithPartitionClause() throws ScriptException {
//
//        // Analytical function Test case 1 : stddev_pop on window with partition
//        /* Input dataset
//        *   +----+----+----+----+----+
//            |Id_1|Id_2|Year|Me_1|Me_2|
//            +----+----+----+----+----+
//            |   A|  XX|2000|   3| 1.0|
//            |   A|  XX|2001|   4| 9.0|
//            |   A|  XX|2002|   7| 5.0|
//            |   A|  XX|2003|   6| 8.0|
//            |   A|  YY|2000|   9| 3.0|
//            |   A|  YY|2001|   5| 4.0|
//            |   A|  YY|2002|  10| 2.0|
//            |   A|  YY|2003|   5| 7.0|
//            +----+----+----+----+----+
//        * */
//        ScriptContext context = engine.getContext();
//        context.setAttribute("ds1", anCountDS1, ScriptContext.ENGINE_SCOPE);
//
//
//        engine.eval("res := stddev_pop ( ds1 over ( partition by Id_1, Id_2 ) );");
//        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);
//
//        /*
//        *
//
//            +----+----+----+----+----+------------------+------------------+
//            |Id_1|Id_2|Year|Me_1|Me_2|      std_pop_Me_1|      std_pop_Me_2|
//            +----+----+----+----+----+------------------+------------------+
//            |   A|  XX|2000|   3| 1.0|1.5811388300841895| 3.112474899497183|
//            |   A|  XX|2001|   4| 9.0|1.5811388300841895| 3.112474899497183|
//            |   A|  XX|2002|   7| 5.0|1.5811388300841895| 3.112474899497183|
//            |   A|  XX|2003|   6| 8.0|1.5811388300841895| 3.112474899497183|
//            |   A|  YY|2000|   9| 3.0| 2.277608394786075|1.8708286933869707|
//            |   A|  YY|2001|   5| 4.0| 2.277608394786075|1.8708286933869707|
//            |   A|  YY|2002|  10| 2.0| 2.277608394786075|1.8708286933869707|
//            |   A|  YY|2003|   5| 7.0| 2.277608394786075|1.8708286933869707|
//            +----+----+----+----+----+------------------+------------------+
//        * */
//        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 1.58D, "Me_2", 3.11D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 1.58D, "Me_2", 3.11D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 1.58D, "Me_2", 3.11D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 1.58D, "Me_2", 3.11D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 2.28D, "Me_2", 1.87D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 2.28D, "Me_2", 1.87D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 2.28D, "Me_2", 1.87D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 2.28D, "Me_2", 1.87D)
//        );
//
//    }
//
//    @Test
//    public void testAnStdPopWithPartitionOrderByClause() throws ScriptException {
//
//        // Analytical function Test case 2 : stddev_pop on window with partition and order by
//        /* Input dataset
//        *   +----+----+----+----+----+
//            |Id_1|Id_2|Year|Me_1|Me_2|
//            +----+----+----+----+----+
//            |   A|  XX|2000|   3| 1.0|
//            |   A|  XX|2001|   4| 9.0|
//            |   A|  XX|2002|   7| 5.0|
//            |   A|  XX|2003|   6| 8.0|
//            |   A|  YY|2000|   9| 3.0|
//            |   A|  YY|2001|   5| 4.0|
//            |   A|  YY|2002|  10| 2.0|
//            |   A|  YY|2003|   5| 7.0|
//            +----+----+----+----+----+
//        * */
//        ScriptContext context = engine.getContext();
//        context.setAttribute("ds1", anCountDS1, ScriptContext.ENGINE_SCOPE);
//
//
//        engine.eval("res := stddev_pop ( ds1 over ( partition by Id_1, Id_2 order by Year) );");
//        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);
//
//        /*
//        +----+----+----+----+----+------------------+------------------+
//        |Id_1|Id_2|Year|Me_1|Me_2|      std_pop_Me_1|      std_pop_Me_2|
//        +----+----+----+----+----+------------------+------------------+
//        |   A|  XX|2000|   3| 1.0|               0.0|               0.0|
//        |   A|  XX|2001|   4| 9.0|               0.5|               4.0|
//        |   A|  XX|2002|   7| 5.0| 1.699673171197595| 3.265986323710904|
//        |   A|  XX|2003|   6| 8.0|1.5811388300841895| 3.112474899497183|
//        |   A|  YY|2000|   9| 3.0|               0.0|               0.0|
//        |   A|  YY|2001|   5| 4.0|               2.0|               0.5|
//        |   A|  YY|2002|  10| 2.0| 2.160246899469287| 0.816496580927726|
//        |   A|  YY|2003|   5| 7.0| 2.277608394786075|1.8708286933869707|
//        +----+----+----+----+----+------------------+------------------+
//
//        * */
//        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 0.0D, "Me_2", 0.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 0.5D, "Me_2", 4.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 1.70D, "Me_2", 3.27D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 1.58D, "Me_2", 3.11D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 0.0D, "Me_2", 0.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 2.0D, "Me_2", 0.5D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 2.16D, "Me_2", 0.82D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 2.28D, "Me_2", 1.87D)
//        );
//
//    }
//
//
//    @Test
//    public void testAnStdPopWithPartitionOrderByDPClause() throws ScriptException {
//
//        // Analytical function count test case 3 : stddev_pop on window with partition, orderBy and data points
//        /* Input dataset
//        *   +----+----+----+----+----+
//            |Id_1|Id_2|Year|Me_1|Me_2|
//            +----+----+----+----+----+
//            |   A|  XX|2000|   3| 1.0|
//            |   A|  XX|2001|   4| 9.0|
//            |   A|  XX|2002|   7| 5.0|
//            |   A|  XX|2003|   6| 8.0|
//            |   A|  YY|2000|   9| 3.0|
//            |   A|  YY|2001|   5| 4.0|
//            |   A|  YY|2002|  10| 2.0|
//            |   A|  YY|2003|   5| 7.0|
//            +----+----+----+----+----+
//        * */
//        ScriptContext context = engine.getContext();
//        context.setAttribute("ds1", anCountDS1, ScriptContext.ENGINE_SCOPE);
//
//        engine.eval("res := stddev_pop ( ds1 over ( partition by Id_1 order by Id_2 data points between 2 preceding and 2 following) );");
//        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);
//
//        /*
//        * The result data frame
//        *
//        +----+----+----+----+----+------------------+------------------+
//        |Id_1|Id_2|Year|Me_1|Me_2|      std_pop_Me_1|      std_pop_Me_2|
//        +----+----+----+----+----+------------------+------------------+
//        |   A|  XX|2000|   3| 1.0| 1.699673171197595| 3.265986323710904|
//        |   A|  XX|2001|   4| 9.0|1.5811388300841895| 3.112474899497183|
//        |   A|  XX|2002|   7| 5.0|2.1354156504062622| 2.993325909419153|
//        |   A|  XX|2003|   6| 8.0|1.7204650534085253|2.3151673805580453|
//        |   A|  YY|2000|   9| 3.0|1.8547236990991407|   2.0591260281974|
//        |   A|  YY|2001|   5| 4.0|2.0976176963403033|2.3151673805580453|
//        |   A|  YY|2002|  10| 2.0| 2.277608394786075|1.8708286933869707|
//        |   A|  YY|2003|   5| 7.0| 2.357022603955158|2.0548046676563256|
//        +----+----+----+----+----+------------------+------------------+
//
//        * */
//        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 1.70D, "Me_2", 3.27D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 1.58D, "Me_2", 3.11D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 2.14D, "Me_2", 2.99D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 1.72D, "Me_2", 2.32D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 1.85D, "Me_2", 2.06D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 2.09D, "Me_2", 2.32D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 2.28D, "Me_2", 1.87D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 2.36D, "Me_2", 2.05D)
//        );
//
//    }
//
//    @Test
//    public void testAnStdPopWithPartitionOrderByRangeClause() throws ScriptException {
//
//        // Analytical function count test case 4 : stddev_pop on window with partition, orderBy and range
//        /* Input dataset
//        *   +----+----+----+----+----+
//            |Id_1|Id_2|Year|Me_1|Me_2|
//            +----+----+----+----+----+
//            |   A|  XX|2000|   3| 1.0|
//            |   A|  XX|2001|   4| 9.0|
//            |   A|  XX|2002|   7| 5.0|
//            |   A|  XX|2003|   6| 8.0|
//            |   A|  YY|2000|   9| 3.0|
//            |   A|  YY|2001|   5| 4.0|
//            |   A|  YY|2002|  10| 2.0|
//            |   A|  YY|2003|   5| 7.0|
//            +----+----+----+----+----+
//        * */
//        ScriptContext context = engine.getContext();
//        context.setAttribute("ds1", anCountDS1, ScriptContext.ENGINE_SCOPE);
//
//
//        engine.eval("res := stddev_pop ( ds1 over ( partition by Id_1 order by Year range between -1 and 1) );");
//        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);
//
//        /*
//        * The result data frame
//        *
//        +----+----+----+----+----+------------------+------------------+
//        |Id_1|Id_2|Year|Me_1|Me_2|      std_pop_Me_1|      std_pop_Me_2|
//        +----+----+----+----+----+------------------+------------------+
//        |   A|  XX|2000|   3| 1.0| 2.277608394786075| 2.947456530637899|
//        |   A|  YY|2000|   9| 3.0| 2.277608394786075| 2.947456530637899|
//        |   A|  XX|2001|   4| 9.0| 2.560381915956203|2.5819888974716116|
//        |   A|  YY|2001|   5| 4.0| 2.560381915956203|2.5819888974716116|
//        |   A|  XX|2002|   7| 5.0|1.9507833184532708|2.4094720491334933|
//        |   A|  YY|2002|  10| 2.0|1.9507833184532708|2.4094720491334933|
//        |   A|  XX|2003|   6| 8.0|1.8708286933869707|  2.29128784747792|
//        |   A|  YY|2003|   5| 7.0|1.8708286933869707|  2.29128784747792|
//        +----+----+----+----+----+------------------+------------------+
//
//        * */
//        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 2.28D, "Me_2", 2.95D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 2.28D, "Me_2", 2.95D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 2.56D, "Me_2", 2.58D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 2.56D, "Me_2", 2.58D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 1.95D, "Me_2", 2.41D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 1.95D, "Me_2", 2.41D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 1.87D, "Me_2", 2.29D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 1.87D, "Me_2", 2.29D)
//        );
//
//    }
//
//    /*
//     * End of stddev_pop test case */
//
//    /*
//     * Test case for analytic function stddev_samp
//     *
//     * */
//
//    @Test
//    public void testAnStdSampWithPartitionClause() throws ScriptException {
//
//        // Analytical function Test case 1 : stddev_samp on window with partition
//        /* Input dataset
//        *   +----+----+----+----+----+
//            |Id_1|Id_2|Year|Me_1|Me_2|
//            +----+----+----+----+----+
//            |   A|  XX|2000|   3| 1.0|
//            |   A|  XX|2001|   4| 9.0|
//            |   A|  XX|2002|   7| 5.0|
//            |   A|  XX|2003|   6| 8.0|
//            |   A|  YY|2000|   9| 3.0|
//            |   A|  YY|2001|   5| 4.0|
//            |   A|  YY|2002|  10| 2.0|
//            |   A|  YY|2003|   5| 7.0|
//            +----+----+----+----+----+
//        * */
//        ScriptContext context = engine.getContext();
//        context.setAttribute("ds1", anCountDS1, ScriptContext.ENGINE_SCOPE);
//
//
//        engine.eval("res := stddev_samp ( ds1 over ( partition by Id_1, Id_2 ) );");
//        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);
//
//        /*
//        *
//
//           +----+----+----+----+----+------------------+-----------------+
//            |Id_1|Id_2|Year|Me_1|Me_2|     std_samp_Me_1|    std_samp_Me_2|
//            +----+----+----+----+----+------------------+-----------------+
//            |   A|  XX|2000|   3| 1.0|1.8257418583505536|3.593976442141304|
//            |   A|  XX|2001|   4| 9.0|1.8257418583505536|3.593976442141304|
//            |   A|  XX|2002|   7| 5.0|1.8257418583505536|3.593976442141304|
//            |   A|  XX|2003|   6| 8.0|1.8257418583505536|3.593976442141304|
//            |   A|  YY|2000|   9| 3.0|2.6299556396765835|2.160246899469287|
//            |   A|  YY|2001|   5| 4.0|2.6299556396765835|2.160246899469287|
//            |   A|  YY|2002|  10| 2.0|2.6299556396765835|2.160246899469287|
//            |   A|  YY|2003|   5| 7.0|2.6299556396765835|2.160246899469287|
//            +----+----+----+----+----+------------------+-----------------+
//        * */
//        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 1.83D, "Me_2", 3.59D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 1.83D, "Me_2", 3.59D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 1.83D, "Me_2", 3.59D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 1.83D, "Me_2", 3.59D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 2.63D, "Me_2", 2.16D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 2.63D, "Me_2", 2.16D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 2.63D, "Me_2", 2.16D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 2.63D, "Me_2", 2.16D)
//        );
//
//    }
//
//    @Test
//    public void testAnStdSampWithPartitionOrderByClause() throws ScriptException {
//
//        // Analytical function Test case 2 : stddev_samp on window with partition and order by
//        /* Input dataset
//        *   +----+----+----+----+----+
//            |Id_1|Id_2|Year|Me_1|Me_2|
//            +----+----+----+----+----+
//            |   A|  XX|2000|   3| 1.0|
//            |   A|  XX|2001|   4| 9.0|
//            |   A|  XX|2002|   7| 5.0|
//            |   A|  XX|2003|   6| 8.0|
//            |   A|  YY|2000|   9| 3.0|
//            |   A|  YY|2001|   5| 4.0|
//            |   A|  YY|2002|  10| 2.0|
//            |   A|  YY|2003|   5| 7.0|
//            +----+----+----+----+----+
//        * */
//        ScriptContext context = engine.getContext();
//        context.setAttribute("ds1", anCountDS1, ScriptContext.ENGINE_SCOPE);
//
//
//        engine.eval("res := stddev_samp ( ds1 over ( partition by Id_1, Id_2 order by Year) );");
//        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);
//
//        /*
//        +----+----+----+----+----+------------------+------------------+
//        |Id_1|Id_2|Year|Me_1|Me_2|     std_samp_Me_1|     std_samp_Me_2|
//        +----+----+----+----+----+------------------+------------------+
//        |   A|  XX|2000|   3| 1.0|              null|              null|
//        |   A|  XX|2001|   4| 9.0|0.7071067811865476| 5.656854249492381|
//        |   A|  XX|2002|   7| 5.0|2.0816659994661326|               4.0|
//        |   A|  XX|2003|   6| 8.0|1.8257418583505536| 3.593976442141304|
//        |   A|  YY|2000|   9| 3.0|              null|              null|
//        |   A|  YY|2001|   5| 4.0|2.8284271247461903|0.7071067811865476|
//        |   A|  YY|2002|  10| 2.0|2.6457513110645907|               1.0|
//        |   A|  YY|2003|   5| 7.0|2.6299556396765835| 2.160246899469287|
//        +----+----+----+----+----+------------------+------------------+
//
//        * */
//
//        /*
//         * todo
//         * Map.of can't contain null key or value
//         *
//         * need another way to create data frame
//         * */
//        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", null, "Me_2", null),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 0.71D, "Me_2", 5.66D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 2.08D, "Me_2", 4.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 1.82D, "Me_2", 3.59D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", null, "Me_2", null),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 2.83D, "Me_2", 0.71D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 2.65D, "Me_2", 1.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 2.63D, "Me_2", 2.16D)
//        );
//
//    }
//
//
//    @Test
//    public void testAnStdSampWithPartitionOrderByDPClause() throws ScriptException {
//
//        // Analytical function count test case 3 : stddev_samp on window with partition, orderBy and data points
//        /* Input dataset
//        *   +----+----+----+----+----+
//            |Id_1|Id_2|Year|Me_1|Me_2|
//            +----+----+----+----+----+
//            |   A|  XX|2000|   3| 1.0|
//            |   A|  XX|2001|   4| 9.0|
//            |   A|  XX|2002|   7| 5.0|
//            |   A|  XX|2003|   6| 8.0|
//            |   A|  YY|2000|   9| 3.0|
//            |   A|  YY|2001|   5| 4.0|
//            |   A|  YY|2002|  10| 2.0|
//            |   A|  YY|2003|   5| 7.0|
//            +----+----+----+----+----+
//        * */
//        ScriptContext context = engine.getContext();
//        context.setAttribute("ds1", anCountDS1, ScriptContext.ENGINE_SCOPE);
//
//        engine.eval("res := stddev_samp ( ds1 over ( partition by Id_1 order by Id_2 data points between 2 preceding and 2 following) );");
//        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);
//
//        /*
//        * The result data frame
//        *
//        +----+----+----+----+----+------------------+------------------+
//        |Id_1|Id_2|Year|Me_1|Me_2|     std_samp_Me_1|     std_samp_Me_2|
//        +----+----+----+----+----+------------------+------------------+
//        |   A|  XX|2000|   3| 1.0|2.0816659994661326|               4.0|
//        |   A|  XX|2001|   4| 9.0|1.8257418583505536| 3.593976442141304|
//        |   A|  XX|2002|   7| 5.0|2.3874672772626644|3.3466401061363023|
//        |   A|  XX|2003|   6| 8.0|1.9235384061671346| 2.588435821108957|
//        |   A|  YY|2000|   9| 3.0| 2.073644135332772|2.3021728866442674|
//        |   A|  YY|2001|   5| 4.0| 2.345207879911715| 2.588435821108957|
//        |   A|  YY|2002|  10| 2.0|2.6299556396765835| 2.160246899469287|
//        |   A|  YY|2003|   5| 7.0|2.8867513459481287|2.5166114784235836|
//        +----+----+----+----+----+------------------+------------------+
//
//
//        * */
//        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 2.08D, "Me_2", 4.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 1.83D, "Me_2", 3.59D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 2.38D, "Me_2", 3.35D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 1.92D, "Me_2", 2.59D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 2.07D, "Me_2", 2.30D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 2.35D, "Me_2", 2.59D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 2.63D, "Me_2", 2.16D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 2.89D, "Me_2", 2.52D)
//        );
//
//    }
//
//    @Test
//    public void testAnStdSampWithPartitionOrderByRangeClause() throws ScriptException {
//
//        // Analytical function count test case 4 : stddev_samp on window with partition, orderBy and range
//        /* Input dataset
//        *   +----+----+----+----+----+
//            |Id_1|Id_2|Year|Me_1|Me_2|
//            +----+----+----+----+----+
//            |   A|  XX|2000|   3| 1.0|
//            |   A|  XX|2001|   4| 9.0|
//            |   A|  XX|2002|   7| 5.0|
//            |   A|  XX|2003|   6| 8.0|
//            |   A|  YY|2000|   9| 3.0|
//            |   A|  YY|2001|   5| 4.0|
//            |   A|  YY|2002|  10| 2.0|
//            |   A|  YY|2003|   5| 7.0|
//            +----+----+----+----+----+
//        * */
//        ScriptContext context = engine.getContext();
//        context.setAttribute("ds1", anCountDS1, ScriptContext.ENGINE_SCOPE);
//
//
//        engine.eval("res := stddev_samp ( ds1 over ( partition by Id_1 order by Year range between -1 and 1) );");
//        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);
//
//        /*
//        * The result data frame
//        *
//        +----+----+----+----+----+------------------+------------------+
//        |Id_1|Id_2|Year|Me_1|Me_2|     std_samp_Me_1|     std_samp_Me_2|
//        +----+----+----+----+----+------------------+------------------+
//        |   A|  XX|2000|   3| 1.0|2.6299556396765835|3.4034296427770228|
//        |   A|  YY|2000|   9| 3.0|2.6299556396765835|3.4034296427770228|
//        |   A|  XX|2001|   4| 9.0|2.8047578623950176|2.8284271247461903|
//        |   A|  YY|2001|   5| 4.0|2.8047578623950176|2.8284271247461903|
//        |   A|  XX|2002|   7| 5.0| 2.136976056643281|2.6394443859772205|
//        |   A|  YY|2002|  10| 2.0| 2.136976056643281|2.6394443859772205|
//        |   A|  XX|2003|   6| 8.0| 2.160246899469287|2.6457513110645907|
//        |   A|  YY|2003|   5| 7.0| 2.160246899469287|2.6457513110645907|
//        +----+----+----+----+----+------------------+------------------+
//
//        * */
//        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 2.63D, "Me_2", 3.40D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 2.63D, "Me_2", 3.40D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 2.80D, "Me_2", 2.83D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 2.80D, "Me_2", 2.83D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 2.14D, "Me_2", 2.64D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 2.14D, "Me_2", 2.64D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 2.16D, "Me_2", 2.65D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 2.16D, "Me_2", 2.65D)
//        );
//
//    }
//
//    /*
//     * End of stddev_samp test case */
//
//    /*
//     * Test case for analytic function var_pop
//     *
//     * */
//    @Test
//    public void testAnVarPopWithPartitionClause() throws ScriptException {
//
//        // Analytical function Test case 1 : var_pop on window with partition
//        /* Input dataset
//        *   +----+----+----+----+----+
//            |Id_1|Id_2|Year|Me_1|Me_2|
//            +----+----+----+----+----+
//            |   A|  XX|2000|   3| 1.0|
//            |   A|  XX|2001|   4| 9.0|
//            |   A|  XX|2002|   7| 5.0|
//            |   A|  XX|2003|   6| 8.0|
//            |   A|  YY|2000|   9| 3.0|
//            |   A|  YY|2001|   5| 4.0|
//            |   A|  YY|2002|  10| 2.0|
//            |   A|  YY|2003|   5| 7.0|
//            +----+----+----+----+----+
//        * */
//        ScriptContext context = engine.getContext();
//        context.setAttribute("ds1", anCountDS1, ScriptContext.ENGINE_SCOPE);
//
//
//        engine.eval("res := var_pop ( ds1 over ( partition by Id_1, Id_2 ) );");
//        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);
//
//        /*
//        *
//
//            +----+----+----+----+----+------------------+------------+
//            |Id_1|Id_2|Year|Me_1|Me_2|      var_pop_Me_1|var_pop_Me_2|
//            +----+----+----+----+----+------------------+------------+
//            |   A|  XX|2000|   3| 1.0|2.4999999999999996|      9.6875|
//            |   A|  XX|2001|   4| 9.0|2.4999999999999996|      9.6875|
//            |   A|  XX|2002|   7| 5.0|2.4999999999999996|      9.6875|
//            |   A|  XX|2003|   6| 8.0|2.4999999999999996|      9.6875|
//            |   A|  YY|2000|   9| 3.0|            5.1875|         3.5|
//            |   A|  YY|2001|   5| 4.0|            5.1875|         3.5|
//            |   A|  YY|2002|  10| 2.0|            5.1875|         3.5|
//            |   A|  YY|2003|   5| 7.0|            5.1875|         3.5|
//            +----+----+----+----+----+------------------+------------+
//        * */
//        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 2.50D, "Me_2", 9.69D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 2.50D, "Me_2", 9.69D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 2.50D, "Me_2", 9.69D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 2.50D, "Me_2", 9.69D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 5.19D, "Me_2", 3.5D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 5.19D, "Me_2", 3.5D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 5.19D, "Me_2", 3.5D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 5.19D, "Me_2", 3.5D)
//        );
//
//    }
//
//    @Test
//    public void testAnVarPopWithPartitionOrderByClause() throws ScriptException {
//
//        // Analytical function Test case 2 : var_pop on window with partition and order by
//        /* Input dataset
//        *   +----+----+----+----+----+
//            |Id_1|Id_2|Year|Me_1|Me_2|
//            +----+----+----+----+----+
//            |   A|  XX|2000|   3| 1.0|
//            |   A|  XX|2001|   4| 9.0|
//            |   A|  XX|2002|   7| 5.0|
//            |   A|  XX|2003|   6| 8.0|
//            |   A|  YY|2000|   9| 3.0|
//            |   A|  YY|2001|   5| 4.0|
//            |   A|  YY|2002|  10| 2.0|
//            |   A|  YY|2003|   5| 7.0|
//            +----+----+----+----+----+
//        * */
//        ScriptContext context = engine.getContext();
//        context.setAttribute("ds1", anCountDS1, ScriptContext.ENGINE_SCOPE);
//
//
//        engine.eval("res := var_pop ( ds1 over ( partition by Id_1, Id_2 order by Year) );");
//        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);
//
//        /*
//        +----+----+----+----+----+------------------+------------------+
//        |Id_1|Id_2|Year|Me_1|Me_2|      var_pop_Me_1|      var_pop_Me_2|
//        +----+----+----+----+----+------------------+------------------+
//        |   A|  XX|2000|   3| 1.0|               0.0|               0.0|
//        |   A|  XX|2001|   4| 9.0|              0.25|              16.0|
//        |   A|  XX|2002|   7| 5.0| 2.888888888888889|10.666666666666666|
//        |   A|  XX|2003|   6| 8.0|2.4999999999999996|            9.6875|
//        |   A|  YY|2000|   9| 3.0|               0.0|               0.0|
//        |   A|  YY|2001|   5| 4.0|               4.0|              0.25|
//        |   A|  YY|2002|  10| 2.0| 4.666666666666667|0.6666666666666666|
//        |   A|  YY|2003|   5| 7.0|            5.1875|               3.5|
//        +----+----+----+----+----+------------------+------------------+
//
//        * */
//
//        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 0.0D, "Me_2", 0.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 0.25D, "Me_2", 16.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 2.89D, "Me_2", 10.67D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 2.50D, "Me_2", 9.69D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 0.0D, "Me_2", 0.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 4.0D, "Me_2", 0.25D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 4.67D, "Me_2", 0.67D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 5.19D, "Me_2", 3.5D)
//        );
//
//    }
//
//
//    @Test
//    public void testAnVarPopWithPartitionOrderByDPClause() throws ScriptException {
//
//        // Analytical function count test case 3 : var_pop on window with partition, orderBy and data points
//        /* Input dataset
//        *   +----+----+----+----+----+
//            |Id_1|Id_2|Year|Me_1|Me_2|
//            +----+----+----+----+----+
//            |   A|  XX|2000|   3| 1.0|
//            |   A|  XX|2001|   4| 9.0|
//            |   A|  XX|2002|   7| 5.0|
//            |   A|  XX|2003|   6| 8.0|
//            |   A|  YY|2000|   9| 3.0|
//            |   A|  YY|2001|   5| 4.0|
//            |   A|  YY|2002|  10| 2.0|
//            |   A|  YY|2003|   5| 7.0|
//            +----+----+----+----+----+
//        * */
//        ScriptContext context = engine.getContext();
//        context.setAttribute("ds1", anCountDS1, ScriptContext.ENGINE_SCOPE);
//
//        engine.eval("res := var_pop ( ds1 over ( partition by Id_1 order by Id_2 data points between 2 preceding and 2 following) );");
//        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);
//
//        /*
//        * The result data frame
//        *
//        +----+----+----+----+----+------------------+------------------+
//        |Id_1|Id_2|Year|Me_1|Me_2|      var_pop_Me_1|      var_pop_Me_2|
//        +----+----+----+----+----+------------------+------------------+
//        |   A|  XX|2000|   3| 1.0| 2.888888888888889|10.666666666666666|
//        |   A|  XX|2001|   4| 9.0|2.4999999999999996|            9.6875|
//        |   A|  XX|2002|   7| 5.0|              4.56| 8.959999999999999|
//        |   A|  XX|2003|   6| 8.0|              2.96|              5.36|
//        |   A|  YY|2000|   9| 3.0|              3.44| 4.239999999999999|
//        |   A|  YY|2001|   5| 4.0|               4.4|              5.36|
//        |   A|  YY|2002|  10| 2.0|            5.1875|               3.5|
//        |   A|  YY|2003|   5| 7.0|5.5555555555555545| 4.222222222222222|
//        +----+----+----+----+----+------------------+------------------+
//
//
//        * */
//        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 2.89D, "Me_2", 10.67D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 2.49D, "Me_2", 9.69D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 4.56D, "Me_2", 8.96D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 2.96D, "Me_2", 5.36D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 3.44D, "Me_2", 4.24D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 4.4D, "Me_2", 5.36D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 5.19D, "Me_2", 3.5D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 5.56D, "Me_2", 4.22D)
//        );
//
//    }
//
//    @Test
//    public void testAnVarPopWithPartitionOrderByRangeClause() throws ScriptException {
//
//        // Analytical function count test case 4 : var_pop on window with partition, orderBy and range
//        /* Input dataset
//        *   +----+----+----+----+----+
//            |Id_1|Id_2|Year|Me_1|Me_2|
//            +----+----+----+----+----+
//            |   A|  XX|2000|   3| 1.0|
//            |   A|  XX|2001|   4| 9.0|
//            |   A|  XX|2002|   7| 5.0|
//            |   A|  XX|2003|   6| 8.0|
//            |   A|  YY|2000|   9| 3.0|
//            |   A|  YY|2001|   5| 4.0|
//            |   A|  YY|2002|  10| 2.0|
//            |   A|  YY|2003|   5| 7.0|
//            +----+----+----+----+----+
//        * */
//        ScriptContext context = engine.getContext();
//        context.setAttribute("ds1", anCountDS1, ScriptContext.ENGINE_SCOPE);
//
//
//        engine.eval("res := var_pop ( ds1 over ( partition by Id_1 order by Year range between -1 and 1) );");
//        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);
//
//        /*
//        * The result data frame
//        *
//        +----+----+----+----+----+------------------+-----------------+
//        |Id_1|Id_2|Year|Me_1|Me_2|      var_pop_Me_1|     var_pop_Me_2|
//        +----+----+----+----+----+------------------+-----------------+
//        |   A|  XX|2000|   3| 1.0|            5.1875|           8.6875|
//        |   A|  YY|2000|   9| 3.0|            5.1875|           8.6875|
//        |   A|  XX|2001|   4| 9.0| 6.555555555555556|6.666666666666668|
//        |   A|  YY|2001|   5| 4.0| 6.555555555555556|6.666666666666668|
//        |   A|  XX|2002|   7| 5.0|3.8055555555555554|5.805555555555556|
//        |   A|  YY|2002|  10| 2.0|3.8055555555555554|5.805555555555556|
//        |   A|  XX|2003|   6| 8.0|               3.5|             5.25|
//        |   A|  YY|2003|   5| 7.0|               3.5|             5.25|
//        +----+----+----+----+----+------------------+-----------------+
//
//        * */
//        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 5.19D, "Me_2", 8.69D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 5.19D, "Me_2", 8.69D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 6.56D, "Me_2", 6.67D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 6.56D, "Me_2", 6.67D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 3.8D, "Me_2", 5.81D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 3.8D, "Me_2", 5.81D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 3.5D, "Me_2", 5.25D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 3.5D, "Me_2", 5.25D)
//        );
//
//    }
//    /*
//     * End of var_pop test case */
//
//
//    /*
//     * Test case for analytic function var_samp
//     *
//     * */
//    @Test
//    public void testAnVarSampWithPartitionClause() throws ScriptException {
//
//        // Analytical function Test case 1 : var_samp on window with partition
//        /* Input dataset
//        *   +----+----+----+----+----+
//            |Id_1|Id_2|Year|Me_1|Me_2|
//            +----+----+----+----+----+
//            |   A|  XX|2000|   3| 1.0|
//            |   A|  XX|2001|   4| 9.0|
//            |   A|  XX|2002|   7| 5.0|
//            |   A|  XX|2003|   6| 8.0|
//            |   A|  YY|2000|   9| 3.0|
//            |   A|  YY|2001|   5| 4.0|
//            |   A|  YY|2002|  10| 2.0|
//            |   A|  YY|2003|   5| 7.0|
//            +----+----+----+----+----+
//        * */
//        ScriptContext context = engine.getContext();
//        context.setAttribute("ds1", anCountDS1, ScriptContext.ENGINE_SCOPE);
//
//
//        engine.eval("res := var_samp ( ds1 over ( partition by Id_1, Id_2 ) );");
//        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);
//
//        /*
//        *
//
//            +----+----+----+----+----+------------------+------------------+
//            |Id_1|Id_2|Year|Me_1|Me_2|     var_samp_Me_1|     var_samp_Me_2|
//            +----+----+----+----+----+------------------+------------------+
//            |   A|  XX|2000|   3| 1.0|3.3333333333333326|12.916666666666666|
//            |   A|  XX|2001|   4| 9.0|3.3333333333333326|12.916666666666666|
//            |   A|  XX|2002|   7| 5.0|3.3333333333333326|12.916666666666666|
//            |   A|  XX|2003|   6| 8.0|3.3333333333333326|12.916666666666666|
//            |   A|  YY|2000|   9| 3.0| 6.916666666666667| 4.666666666666667|
//            |   A|  YY|2001|   5| 4.0| 6.916666666666667| 4.666666666666667|
//            |   A|  YY|2002|  10| 2.0| 6.916666666666667| 4.666666666666667|
//            |   A|  YY|2003|   5| 7.0| 6.916666666666667| 4.666666666666667|
//            +----+----+----+----+----+------------------+------------------+
//        * */
//        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 3.33D, "Me_2", 12.91D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 3.33D, "Me_2", 12.91D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 3.33D, "Me_2", 12.91D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 3.33D, "Me_2", 12.91D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 6.92D, "Me_2", 4.67D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 6.92D, "Me_2", 4.67D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 6.92D, "Me_2", 4.67D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 6.92D, "Me_2", 4.67D)
//        );
//
//    }
//
//    @Test
//    public void testAnVarSampWithPartitionOrderByClause() throws ScriptException {
//
//        // Analytical function Test case 2 : var_samp on window with partition and order by
//        /* Input dataset
//        *   +----+----+----+----+----+
//            |Id_1|Id_2|Year|Me_1|Me_2|
//            +----+----+----+----+----+
//            |   A|  XX|2000|   3| 1.0|
//            |   A|  XX|2001|   4| 9.0|
//            |   A|  XX|2002|   7| 5.0|
//            |   A|  XX|2003|   6| 8.0|
//            |   A|  YY|2000|   9| 3.0|
//            |   A|  YY|2001|   5| 4.0|
//            |   A|  YY|2002|  10| 2.0|
//            |   A|  YY|2003|   5| 7.0|
//            +----+----+----+----+----+
//        * */
//        ScriptContext context = engine.getContext();
//        context.setAttribute("ds1", anCountDS1, ScriptContext.ENGINE_SCOPE);
//
//
//        engine.eval("res := var_samp ( ds1 over ( partition by Id_1, Id_2 order by Year) );");
//        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);
//
//        /*
//        +----+----+----+----+----+------------------+------------------+
//        |Id_1|Id_2|Year|Me_1|Me_2|     var_samp_Me_1|     var_samp_Me_2|
//        +----+----+----+----+----+------------------+------------------+
//        |   A|  XX|2000|   3| 1.0|              null|              null|
//        |   A|  XX|2001|   4| 9.0|               0.5|              32.0|
//        |   A|  XX|2002|   7| 5.0| 4.333333333333333|              16.0|
//        |   A|  XX|2003|   6| 8.0|3.3333333333333326|12.916666666666666|
//        |   A|  YY|2000|   9| 3.0|              null|              null|
//        |   A|  YY|2001|   5| 4.0|               8.0|               0.5|
//        |   A|  YY|2002|  10| 2.0|               7.0|               1.0|
//        |   A|  YY|2003|   5| 7.0| 6.916666666666667| 4.666666666666667|
//        +----+----+----+----+----+------------------+------------------+
//
//        * */
//        /*
//         * todo
//         * Map.of can't contain null key or value
//         *
//         * need another way to create data frame
//         * */
//        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", null, "Me_2", null),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 0.5D, "Me_2", 32.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 4.33D, "Me_2", 16.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 3.33D, "Me_2", 12.92D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", null, "Me_2", null),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 8.0D, "Me_2", 0.5D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 7.0D, "Me_2", 1.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 6.92D, "Me_2", 4.67D)
//        );
//
//    }
//
//
//    @Test
//    public void testAnVarSampWithPartitionOrderByDPClause() throws ScriptException {
//
//        // Analytical function count test case 3 : var_samp on window with partition, orderBy and data points
//        /* Input dataset
//        *   +----+----+----+----+----+
//            |Id_1|Id_2|Year|Me_1|Me_2|
//            +----+----+----+----+----+
//            |   A|  XX|2000|   3| 1.0|
//            |   A|  XX|2001|   4| 9.0|
//            |   A|  XX|2002|   7| 5.0|
//            |   A|  XX|2003|   6| 8.0|
//            |   A|  YY|2000|   9| 3.0|
//            |   A|  YY|2001|   5| 4.0|
//            |   A|  YY|2002|  10| 2.0|
//            |   A|  YY|2003|   5| 7.0|
//            +----+----+----+----+----+
//        * */
//        ScriptContext context = engine.getContext();
//        context.setAttribute("ds1", anCountDS1, ScriptContext.ENGINE_SCOPE);
//
//        engine.eval("res := var_samp ( ds1 over ( partition by Id_1 order by Id_2 data points between 2 preceding and 2 following) );");
//        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);
//
//        /*
//        * The result data frame
//        *
//        +----+----+----+----+----+------------------+------------------+
//        |Id_1|Id_2|Year|Me_1|Me_2|     var_samp_Me_1|     var_samp_Me_2|
//        +----+----+----+----+----+------------------+------------------+
//        |   A|  XX|2000|   3| 1.0| 4.333333333333333|              16.0|
//        |   A|  XX|2001|   4| 9.0|3.3333333333333326|12.916666666666666|
//        |   A|  XX|2002|   7| 5.0| 5.699999999999999|              11.2|
//        |   A|  XX|2003|   6| 8.0|               3.7|               6.7|
//        |   A|  YY|2000|   9| 3.0|               4.3| 5.299999999999999|
//        |   A|  YY|2001|   5| 4.0|               5.5|               6.7|
//        |   A|  YY|2002|  10| 2.0| 6.916666666666667| 4.666666666666667|
//        |   A|  YY|2003|   5| 7.0| 8.333333333333332| 6.333333333333334|
//        +----+----+----+----+----+------------------+------------------+
//
//
//
//        * */
//        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 4.33D, "Me_2", 16.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 3.33D, "Me_2", 12.92D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 5.70D, "Me_2", 11.2D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 3.7D, "Me_2", 6.7D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 4.3D, "Me_2", 5.3D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 5.5D, "Me_2", 6.7D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 6.92D, "Me_2", 4.67D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 8.33D, "Me_2", 6.33D)
//        );
//
//    }
//
//    @Test
//    public void testAnVarSampWithPartitionOrderByRangeClause() throws ScriptException {
//
//        // Analytical function count test case 4 : var_samp on window with partition, orderBy and range
//        /* Input dataset
//        *   +----+----+----+----+----+
//            |Id_1|Id_2|Year|Me_1|Me_2|
//            +----+----+----+----+----+
//            |   A|  XX|2000|   3| 1.0|
//            |   A|  XX|2001|   4| 9.0|
//            |   A|  XX|2002|   7| 5.0|
//            |   A|  XX|2003|   6| 8.0|
//            |   A|  YY|2000|   9| 3.0|
//            |   A|  YY|2001|   5| 4.0|
//            |   A|  YY|2002|  10| 2.0|
//            |   A|  YY|2003|   5| 7.0|
//            +----+----+----+----+----+
//        * */
//        ScriptContext context = engine.getContext();
//        context.setAttribute("ds1", anCountDS1, ScriptContext.ENGINE_SCOPE);
//
//
//        engine.eval("res := var_pop ( ds1 over ( partition by Id_1 order by Year range between -1 and 1) );");
//        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);
//
//        /*
//        * The result data frame
//        *
//        +----+----+----+----+----+-----------------+------------------+
//        |Id_1|Id_2|Year|Me_1|Me_2|    var_samp_Me_1|     var_samp_Me_2|
//        +----+----+----+----+----+-----------------+------------------+
//        |   A|  XX|2000|   3| 1.0|6.916666666666667|11.583333333333334|
//        |   A|  YY|2000|   9| 3.0|6.916666666666667|11.583333333333334|
//        |   A|  XX|2001|   4| 9.0|7.866666666666667| 8.000000000000002|
//        |   A|  YY|2001|   5| 4.0|7.866666666666667| 8.000000000000002|
//        |   A|  XX|2002|   7| 5.0|4.566666666666666| 6.966666666666667|
//        |   A|  YY|2002|  10| 2.0|4.566666666666666| 6.966666666666667|
//        |   A|  XX|2003|   6| 8.0|4.666666666666667|               7.0|
//        |   A|  YY|2003|   5| 7.0|4.666666666666667|               7.0|
//        +----+----+----+----+----+-----------------+------------------+
//
//        * */
//        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 6.92D, "Me_2", 11.58D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 6.92D, "Me_2", 11.58D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 7.87D, "Me_2", 8.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 7.87D, "Me_2", 8.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 4.57D, "Me_2", 6.97D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 4.57D, "Me_2", 6.97D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 4.67D, "Me_2", 7.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 4.67D, "Me_2", 7.0D)
//        );
//
//    }
//    /*
//     * End of var_samp test case */
//
//
//    /*
//     * Test case for analytic function rank
//     *  ** rank analytic clause restriction**
//     *  - Must have `orderClause`
//     * - The `windowClause` such as `data points` and `range` are not allowed
//     * */
//    @Test
//    public void testAnRankAscClause() throws ScriptException {
//
//        // Analytical function Test case 1 : rank on window with partition and asc order
//        /* Input dataset
//        +----+----+----+----+----+
//        |Id_1|Id_2|Year|Me_1|Me_2|
//        +----+----+----+----+----+
//        |   A|  XX|1993|   3| 1.0|
//        |   A|  XX|1994|   4| 9.0|
//        |   A|  XX|1995|   7| 5.0|
//        |   A|  XX|1996|   6| 8.0|
//        |   A|  YY|1993|   9| 3.0|
//        |   A|  YY|1994|   5| 4.0|
//        |   A|  YY|1995|  10| 2.0|
//        |   A|  YY|1996|   2| 7.0|
//        +----+----+----+----+----+
//
//        * */
//        ScriptContext context = engine.getContext();
//        context.setAttribute("ds2", anCountDS2, ScriptContext.ENGINE_SCOPE);
//
//
//        engine.eval("res := ds2 [calc toto:= rank ( over ( partition by Id_1, Id_2 order by Year) )];");
//        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);
//
//        /*
//        *
//        +----+----+----+----+----+--------+
//        |Id_1|Id_2|Year|Me_1|Me_2|rank_col|
//        +----+----+----+----+----+--------+
//        |   A|  XX|1993|   3| 1.0|       1|
//        |   A|  XX|1994|   4| 9.0|       2|
//        |   A|  XX|1995|   7| 5.0|       3|
//        |   A|  XX|1996|   6| 8.0|       4|
//        |   A|  YY|1993|   9| 3.0|       1|
//        |   A|  YY|1994|   5| 4.0|       2|
//        |   A|  YY|1995|  10| 2.0|       3|
//        |   A|  YY|1996|   2| 7.0|       4|
//        +----+----+----+----+----+--------+
//        * */
//        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 1993L, "Me_1", 3L, "Me_2", 1.0D, "rank_col", 1L),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 1994L, "Me_1", 4L, "Me_2", 9.0D, "rank_col", 2L),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 1995L, "Me_1", 7L, "Me_2", 5.0D, "rank_col", 3L),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 1996L, "Me_1", 6L, "Me_2", 8.0D, "rank_col", 4L),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 1993L, "Me_1", 9L, "Me_2", 3.0D, "rank_col", 1L),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 1994L, "Me_1", 5L, "Me_2", 4.0D, "rank_col", 2L),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 1995L, "Me_1", 10L, "Me_2", 2.0D, "rank_col", 3L),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 1996L, "Me_1", 2L, "Me_2", 7.0D, "rank_col", 4L)
//        );
//
//    }
//
//    @Test
//    public void testAnRankDesc() throws ScriptException {
//
//        // Analytical function Test case 2 : rank on window with partition and desc order
//        /* Input dataset
//        +----+----+----+----+----+
//        |Id_1|Id_2|Year|Me_1|Me_2|
//        +----+----+----+----+----+
//        |   A|  XX|1993|   3| 1.0|
//        |   A|  XX|1994|   4| 9.0|
//        |   A|  XX|1995|   7| 5.0|
//        |   A|  XX|1996|   6| 8.0|
//        |   A|  YY|1993|   9| 3.0|
//        |   A|  YY|1994|   5| 4.0|
//        |   A|  YY|1995|  10| 2.0|
//        |   A|  YY|1996|   2| 7.0|
//        +----+----+----+----+----+
//
//        * */
//        ScriptContext context = engine.getContext();
//        context.setAttribute("ds2", anCountDS2, ScriptContext.ENGINE_SCOPE);
//
//
//        engine.eval("res := ds2 [calc rank_col:= rank ( over ( partition by Id_1, Id_2 order by Year desc) )];");
//        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);
//
//        /*
//        *
//        +----+----+----+----+----+--------+
//        |Id_1|Id_2|Year|Me_1|Me_2|rank_col|
//        +----+----+----+----+----+--------+
//        |   A|  XX|1996|   6| 8.0|       1|
//        |   A|  XX|1995|   7| 5.0|       2|
//        |   A|  XX|1994|   4| 9.0|       3|
//        |   A|  XX|1993|   3| 1.0|       4|
//        |   A|  YY|1996|   2| 7.0|       1|
//        |   A|  YY|1995|  10| 2.0|       2|
//        |   A|  YY|1994|   5| 4.0|       3|
//        |   A|  YY|1993|   9| 3.0|       4|
//        +----+----+----+----+----+--------+
//        * */
//        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 1996L, "Me_1", 6L, "Me_2", 8.0D, "rank_col", 1L),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 1995L, "Me_1", 7L, "Me_2", 5.0D, "rank_col", 2L),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 1994L, "Me_1", 4L, "Me_2", 9.0D, "rank_col", 3L),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 1993L, "Me_1", 3L, "Me_2", 1.0D, "rank_col", 4L),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 1996L, "Me_1", 2L, "Me_2", 7.0D, "rank_col", 1L),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 1995L, "Me_1", 10L, "Me_2", 2.0D, "rank_col", 2L),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 1994L, "Me_1", 5L, "Me_2", 4.0D, "rank_col", 3L),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 1993L, "Me_1", 9L, "Me_2", 3.0D, "rank_col", 4L)
//        );
//
//    }
//    /*
//     * End of rank test case */
//
//    /*
//     * Test case for analytic function first
//     *
//     * */
//
//    @Test
//    public void testAnFirstWithPartitionClause() throws ScriptException {
//
//        // Analytical function Test case 1 : first on window with partition
//        /* Input dataset
//        +----+----+----+----+----+
//        |Id_1|Id_2|Year|Me_1|Me_2|
//        +----+----+----+----+----+
//        |   A|  XX|1993|   3| 1.0|
//        |   A|  XX|1994|   4| 9.0|
//        |   A|  XX|1995|   7| 5.0|
//        |   A|  XX|1996|   6| 8.0|
//        |   A|  YY|1993|   9| 3.0|
//        |   A|  YY|1994|   5| 4.0|
//        |   A|  YY|1995|  10| 2.0|
//        |   A|  YY|1996|   2| 7.0|
//        +----+----+----+----+----+
//
//        * */
//        ScriptContext context = engine.getContext();
//        context.setAttribute("ds2", anCountDS2, ScriptContext.ENGINE_SCOPE);
//
//
//        engine.eval("res :=  first_value ( ds2 over ( partition by Id_1, Id_2) );");
//        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);
//
//        /*
//        *
//        +----+----+----+----+----+----------+----------+
//        |Id_1|Id_2|Year|Me_1|Me_2|first_Me_1|first_Me_2|
//        +----+----+----+----+----+----------+----------+
//        |   A|  XX|1993|   3| 1.0|         3|       1.0|
//        |   A|  XX|1994|   4| 9.0|         3|       1.0|
//        |   A|  XX|1995|   7| 5.0|         3|       1.0|
//        |   A|  XX|1996|   6| 8.0|         3|       1.0|
//        |   A|  YY|1993|   9| 3.0|         9|       3.0|
//        |   A|  YY|1994|   5| 4.0|         9|       3.0|
//        |   A|  YY|1995|  10| 2.0|         9|       3.0|
//        |   A|  YY|1996|   2| 7.0|         9|       3.0|
//        +----+----+----+----+----+----------+----------+
//        * */
//        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 1993L, "Me_1", 3L, "Me_2", 1.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 1994L, "Me_1", 3L, "Me_2", 1.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 1995L, "Me_1", 3L, "Me_2", 1.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 1996L, "Me_1", 3L, "Me_2", 1.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 1993L, "Me_1", 9L, "Me_2", 3.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 1994L, "Me_1", 9L, "Me_2", 3.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 1995L, "Me_1", 9L, "Me_2", 3.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 1996L, "Me_1", 9L, "Me_2", 3.0D)
//        );
//
//    }
//
//    @Test
//    public void testAnFirstPartitionOrderByDesc() throws ScriptException {
//
//        // Analytical function Test case 2 : first on window with partition and desc order
//        /* Input dataset
//        +----+----+----+----+----+
//        |Id_1|Id_2|Year|Me_1|Me_2|
//        +----+----+----+----+----+
//        |   A|  XX|1993|   3| 1.0|
//        |   A|  XX|1994|   4| 9.0|
//        |   A|  XX|1995|   7| 5.0|
//        |   A|  XX|1996|   6| 8.0|
//        |   A|  YY|1993|   9| 3.0|
//        |   A|  YY|1994|   5| 4.0|
//        |   A|  YY|1995|  10| 2.0|
//        |   A|  YY|1996|   2| 7.0|
//        +----+----+----+----+----+
//        * */
//        ScriptContext context = engine.getContext();
//        context.setAttribute("ds2", anCountDS2, ScriptContext.ENGINE_SCOPE);
//
//
//        engine.eval("res :=  first_value ( ds2 over ( partition by Id_1, Id_2 order by Year desc) );");
//        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);
//
//        /*
//        *
//        +----+----+----+----+----+----------+----------+
//        |Id_1|Id_2|Year|Me_1|Me_2|first_Me_1|first_Me_2|
//        +----+----+----+----+----+----------+----------+
//        |   A|  XX|1996|   6| 8.0|         6|       8.0|
//        |   A|  XX|1995|   7| 5.0|         6|       8.0|
//        |   A|  XX|1994|   4| 9.0|         6|       8.0|
//        |   A|  XX|1993|   3| 1.0|         6|       8.0|
//        |   A|  YY|1996|   2| 7.0|         2|       7.0|
//        |   A|  YY|1995|  10| 2.0|         2|       7.0|
//        |   A|  YY|1994|   5| 4.0|         2|       7.0|
//        |   A|  YY|1993|   9| 3.0|         2|       7.0|
//        +----+----+----+----+----+----------+----------+
//        * */
//        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 1996L, "Me_1", 6L, "Me_2", 8.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 1995L, "Me_1", 6L, "Me_2", 8.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 1994L, "Me_1", 6L, "Me_2", 8.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 1993L, "Me_1", 6L, "Me_2", 8.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 1996L, "Me_1", 2L, "Me_2", 7.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 1995L, "Me_1", 2L, "Me_2", 7.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 1994L, "Me_1", 2L, "Me_2", 7.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 1993L, "Me_1", 2L, "Me_2", 7.0D)
//        );
//
//    }
//
//    @Test
//    public void testAnFirstWithPartitionOrderByDPClause() throws ScriptException {
//
//        // Analytical function Test case 3 : first on window with partition, order by and data points
//        /* Input dataset
//        +----+----+----+----+----+
//        |Id_1|Id_2|Year|Me_1|Me_2|
//        +----+----+----+----+----+
//        |   A|  XX|1993|   3| 1.0|
//        |   A|  XX|1994|   4| 9.0|
//        |   A|  XX|1995|   7| 5.0|
//        |   A|  XX|1996|   6| 8.0|
//        |   A|  YY|1993|   9| 3.0|
//        |   A|  YY|1994|   5| 4.0|
//        |   A|  YY|1995|  10| 2.0|
//        |   A|  YY|1996|   2| 7.0|
//        +----+----+----+----+----+
//
//        * */
//        ScriptContext context = engine.getContext();
//        context.setAttribute("ds2", anCountDS2, ScriptContext.ENGINE_SCOPE);
//
//
//        engine.eval("res := first_value ( ds2 over ( partition by Id_1 order by Id_2 data points between 2 preceding and 2 following) );");
//        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);
//
//        /*
//        *
//        +----+----+----+----+----+----------+----------+
//        |Id_1|Id_2|Year|Me_1|Me_2|first_Me_1|first_Me_2|
//        +----+----+----+----+----+----------+----------+
//        |   A|  XX|1993|   3| 1.0|         3|       1.0|
//        |   A|  XX|1994|   4| 9.0|         3|       1.0|
//        |   A|  XX|1995|   7| 5.0|         3|       1.0|
//        |   A|  XX|1996|   6| 8.0|         4|       9.0|
//        |   A|  YY|1993|   9| 3.0|         7|       5.0|
//        |   A|  YY|1994|   5| 4.0|         6|       8.0|
//        |   A|  YY|1995|  10| 2.0|         9|       3.0|
//        |   A|  YY|1996|   2| 7.0|         5|       4.0|
//        +----+----+----+----+----+----------+----------+
//        * */
//        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 1993L, "Me_1", 3L, "Me_2", 1.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 1994L, "Me_1", 3L, "Me_2", 1.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 1995L, "Me_1", 3L, "Me_2", 1.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 1996L, "Me_1", 4L, "Me_2", 9.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 1993L, "Me_1", 7L, "Me_2", 5.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 1994L, "Me_1", 6L, "Me_2", 8.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 1995L, "Me_1", 9L, "Me_2", 3.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 1996L, "Me_1", 5L, "Me_2", 4.0D)
//        );
//
//    }
//
//    @Test
//    public void testAnFirstPartitionOrderByRangeClause() throws ScriptException {
//
//        // Analytical function Test case 4 : first on window with partition, order by and range
//        /* Input dataset
//        +----+----+----+----+----+
//        |Id_1|Id_2|Year|Me_1|Me_2|
//        +----+----+----+----+----+
//        |   A|  XX|1993|   3| 1.0|
//        |   A|  XX|1994|   4| 9.0|
//        |   A|  XX|1995|   7| 5.0|
//        |   A|  XX|1996|   6| 8.0|
//        |   A|  YY|1993|   9| 3.0|
//        |   A|  YY|1994|   5| 4.0|
//        |   A|  YY|1995|  10| 2.0|
//        |   A|  YY|1996|   2| 7.0|
//        +----+----+----+----+----+
//        * */
//        ScriptContext context = engine.getContext();
//        context.setAttribute("ds2", anCountDS2, ScriptContext.ENGINE_SCOPE);
//
//
//        engine.eval("res := first_value ( ds2 over ( partition by Id_1 order by Year range between -1 and 1) );");
//        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);
//
//        /*
//        *
//        +----+----+----+----+----+----------+----------+
//        |Id_1|Id_2|Year|Me_1|Me_2|first_Me_1|first_Me_2|
//        +----+----+----+----+----+----------+----------+
//        |   A|  XX|1993|   3| 1.0|         3|       1.0|
//        |   A|  YY|1993|   9| 3.0|         3|       1.0|
//        |   A|  XX|1994|   4| 9.0|         3|       1.0|
//        |   A|  YY|1994|   5| 4.0|         3|       1.0|
//        |   A|  XX|1995|   7| 5.0|         4|       9.0|
//        |   A|  YY|1995|  10| 2.0|         4|       9.0|
//        |   A|  XX|1996|   6| 8.0|         7|       5.0|
//        |   A|  YY|1996|   2| 7.0|         7|       5.0|
//        +----+----+----+----+----+----------+----------+
//        * */
//        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 1993L, "Me_1", 3L, "Me_2", 1.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 1994L, "Me_1", 3L, "Me_2", 1.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 1995L, "Me_1", 3L, "Me_2", 1.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 1996L, "Me_1", 3L, "Me_2", 1.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 1993L, "Me_1", 4L, "Me_2", 9.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 1994L, "Me_1", 4L, "Me_2", 9.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 1995L, "Me_1", 7L, "Me_2", 5.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 1996L, "Me_1", 7L, "Me_2", 5.0D)
//        );
//
//    }
//
//    /*
//     * End of first test case */
//
//    /*
//     * Test case for analytic function last
//     *
//     * */
//    @Test
//    public void testAnLastWithPartitionClause() throws ScriptException {
//
//        // Analytical function Test case 1 : last on window with partition
//        /* Input dataset
//        +----+----+----+----+----+
//        |Id_1|Id_2|Year|Me_1|Me_2|
//        +----+----+----+----+----+
//        |   A|  XX|1993|   3| 1.0|
//        |   A|  XX|1994|   4| 9.0|
//        |   A|  XX|1995|   7| 5.0|
//        |   A|  XX|1996|   6| 8.0|
//        |   A|  YY|1993|   9| 3.0|
//        |   A|  YY|1994|   5| 4.0|
//        |   A|  YY|1995|  10| 2.0|
//        |   A|  YY|1996|   2| 7.0|
//        +----+----+----+----+----+
//
//        * */
//        ScriptContext context = engine.getContext();
//        context.setAttribute("ds2", anCountDS2, ScriptContext.ENGINE_SCOPE);
//
//
//        engine.eval("res :=  last_value ( ds2 over ( partition by Id_1, Id_2) )");
//        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);
//
//        /*
//        *
//        +----+----+----+----+----+---------+---------+
//        |Id_1|Id_2|Year|Me_1|Me_2|last_Me_1|last_Me_2|
//        +----+----+----+----+----+---------+---------+
//        |   A|  XX|1993|   3| 1.0|        6|      8.0|
//        |   A|  XX|1994|   4| 9.0|        6|      8.0|
//        |   A|  XX|1995|   7| 5.0|        6|      8.0|
//        |   A|  XX|1996|   6| 8.0|        6|      8.0|
//        |   A|  YY|1993|   9| 3.0|        2|      7.0|
//        |   A|  YY|1994|   5| 4.0|        2|      7.0|
//        |   A|  YY|1995|  10| 2.0|        2|      7.0|
//        |   A|  YY|1996|   2| 7.0|        2|      7.0|
//        +----+----+----+----+----+---------+---------+
//        * */
//        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 1993L, "Me_1", 6L, "Me_2", 8.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 1994L, "Me_1", 6L, "Me_2", 8.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 1995L, "Me_1", 6L, "Me_2", 8.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 1996L, "Me_1", 6L, "Me_2", 8.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 1993L, "Me_1", 2L, "Me_2", 7.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 1994L, "Me_1", 2L, "Me_2", 7.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 1995L, "Me_1", 2L, "Me_2", 7.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 1996L, "Me_1", 2L, "Me_2", 7.0D)
//        );
//
//    }
//
//    @Test
//    public void testAnLastPartitionOrderByDesc() throws ScriptException {
//
//        // Analytical function Test case 2 : last on window with partition and desc order
//        /* Input dataset
//        +----+----+----+----+----+
//        |Id_1|Id_2|Year|Me_1|Me_2|
//        +----+----+----+----+----+
//        |   A|  XX|1993|   3| 1.0|
//        |   A|  XX|1994|   4| 9.0|
//        |   A|  XX|1995|   7| 5.0|
//        |   A|  XX|1996|   6| 8.0|
//        |   A|  YY|1993|   9| 3.0|
//        |   A|  YY|1994|   5| 4.0|
//        |   A|  YY|1995|  10| 2.0|
//        |   A|  YY|1996|   2| 7.0|
//        +----+----+----+----+----+
//        * */
//        ScriptContext context = engine.getContext();
//        context.setAttribute("ds2", anCountDS2, ScriptContext.ENGINE_SCOPE);
//
//
//        engine.eval("res :=  last_value ( ds2 over ( partition by Id_1, Id_2 order by Year desc) );");
//        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);
//
//        /*
//        *
//        +----+----+----+----+----+---------+---------+
//        |Id_1|Id_2|Year|Me_1|Me_2|last_Me_1|last_Me_2|
//        +----+----+----+----+----+---------+---------+
//        |   A|  XX|1996|   6| 8.0|        6|      8.0|
//        |   A|  XX|1995|   7| 5.0|        7|      5.0|
//        |   A|  XX|1994|   4| 9.0|        4|      9.0|
//        |   A|  XX|1993|   3| 1.0|        3|      1.0|
//        |   A|  YY|1996|   2| 7.0|        2|      7.0|
//        |   A|  YY|1995|  10| 2.0|       10|      2.0|
//        |   A|  YY|1994|   5| 4.0|        5|      4.0|
//        |   A|  YY|1993|   9| 3.0|        9|      3.0|
//        +----+----+----+----+----+---------+---------+
//        * */
//        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 1996L, "Me_1", 6L, "Me_2", 8.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 1995L, "Me_1", 7L, "Me_2", 5.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 1994L, "Me_1", 4L, "Me_2", 9.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 1993L, "Me_1", 3L, "Me_2", 1.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 1996L, "Me_1", 2L, "Me_2", 7.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 1995L, "Me_1", 10L, "Me_2", 2.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 1994L, "Me_1", 5L, "Me_2", 4.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 1993L, "Me_1", 9L, "Me_2", 3.0D)
//        );
//
//    }
//
//    @Test
//    public void testAnLastWithPartitionOrderByDPClause() throws ScriptException {
//
//        // Analytical function Test case 3 : last on window with partition, order by and data points
//        /* Input dataset
//        +----+----+----+----+----+
//        |Id_1|Id_2|Year|Me_1|Me_2|
//        +----+----+----+----+----+
//        |   A|  XX|1993|   3| 1.0|
//        |   A|  XX|1994|   4| 9.0|
//        |   A|  XX|1995|   7| 5.0|
//        |   A|  XX|1996|   6| 8.0|
//        |   A|  YY|1993|   9| 3.0|
//        |   A|  YY|1994|   5| 4.0|
//        |   A|  YY|1995|  10| 2.0|
//        |   A|  YY|1996|   2| 7.0|
//        +----+----+----+----+----+
//
//        * */
//        ScriptContext context = engine.getContext();
//        context.setAttribute("ds2", anCountDS2, ScriptContext.ENGINE_SCOPE);
//
//
//        engine.eval("res := last_value ( ds2 over ( partition by Id_1 order by Id_2 data points between 2 preceding and 2 following) );");
//        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);
//
//        /*
//        *
//        +----+----+----+----+----+---------+---------+
//        |Id_1|Id_2|Year|Me_1|Me_2|last_Me_1|last_Me_2|
//        +----+----+----+----+----+---------+---------+
//        |   A|  XX|1993|   3| 1.0|        7|      5.0|
//        |   A|  XX|1994|   4| 9.0|        6|      8.0|
//        |   A|  XX|1995|   7| 5.0|        9|      3.0|
//        |   A|  XX|1996|   6| 8.0|        5|      4.0|
//        |   A|  YY|1993|   9| 3.0|       10|      2.0|
//        |   A|  YY|1994|   5| 4.0|        2|      7.0|
//        |   A|  YY|1995|  10| 2.0|        2|      7.0|
//        |   A|  YY|1996|   2| 7.0|        2|      7.0|
//        +----+----+----+----+----+---------+---------+
//        * */
//        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 1993L, "Me_1", 7L, "Me_2", 5.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 1994L, "Me_1", 6L, "Me_2", 8.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 1995L, "Me_1", 9L, "Me_2", 3.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 1996L, "Me_1", 5L, "Me_2", 4.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 1993L, "Me_1", 10L, "Me_2", 2.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 1994L, "Me_1", 2L, "Me_2", 7.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 1995L, "Me_1", 2L, "Me_2", 7.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 1996L, "Me_1", 2L, "Me_2", 7.0D)
//        );
//
//    }
//
//    @Test
//    public void testAnLastPartitionOrderByRangeClause() throws ScriptException {
//
//        // Analytical function Test case 4 : last on window with partition, order by and range
//        /* Input dataset
//        +----+----+----+----+----+
//        |Id_1|Id_2|Year|Me_1|Me_2|
//        +----+----+----+----+----+
//        |   A|  XX|1993|   3| 1.0|
//        |   A|  XX|1994|   4| 9.0|
//        |   A|  XX|1995|   7| 5.0|
//        |   A|  XX|1996|   6| 8.0|
//        |   A|  YY|1993|   9| 3.0|
//        |   A|  YY|1994|   5| 4.0|
//        |   A|  YY|1995|  10| 2.0|
//        |   A|  YY|1996|   2| 7.0|
//        +----+----+----+----+----+
//        * */
//        ScriptContext context = engine.getContext();
//        context.setAttribute("ds2", anCountDS2, ScriptContext.ENGINE_SCOPE);
//
//
//        engine.eval("res := last_value ( ds1 over ( partition by Id_1, Id_2 order by Year range between -1 and 1) );");
//        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);
//
//        /*
//        *
//        +----+----+----+----+----+---------+---------+
//        |Id_1|Id_2|Year|Me_1|Me_2|last_Me_1|last_Me_2|
//        +----+----+----+----+----+---------+---------+
//        |   A|  XX|1993|   3| 1.0|        4|      9.0|
//        |   A|  XX|1994|   4| 9.0|        7|      5.0|
//        |   A|  XX|1995|   7| 5.0|        6|      8.0|
//        |   A|  XX|1996|   6| 8.0|        6|      8.0|
//        |   A|  YY|1993|   9| 3.0|        5|      4.0|
//        |   A|  YY|1994|   5| 4.0|       10|      2.0|
//        |   A|  YY|1995|  10| 2.0|        2|      7.0|
//        |   A|  YY|1996|   2| 7.0|        2|      7.0|
//        +----+----+----+----+----+---------+---------+
//        * */
//        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 1993L, "Me_1", 4L, "Me_2", 9.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 1994L, "Me_1", 7L, "Me_2", 5.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 1995L, "Me_1", 6L, "Me_2", 8.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 1996L, "Me_1", 6L, "Me_2", 8.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 1993L, "Me_1", 5L, "Me_2", 4.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 1994L, "Me_1", 10L, "Me_2", 2.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 1995L, "Me_1", 2L, "Me_2", 7.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 1996L, "Me_1", 2L, "Me_2", 7.0D)
//        );
//
//    }
//    /*
//     * End of last test case */
//
//    /*
//     * Test case for analytic function lead
//     * The lead function take two argument:
//     * - input dataframe
//     * - step
//     * Analytic clause restriction:
//     * - Must have orderClause
//     * - The windowClause such as data points and range are not allowed
//     * */
//
//    @Test
//    public void testAnLead() throws ScriptException {
//
//        // Analytical function Test case 1 : lead on window with partition, order by and range
//        /* Input dataset
//        +----+----+----+----+----+
//        |Id_1|Id_2|Year|Me_1|Me_2|
//        +----+----+----+----+----+
//        |   A|  XX|1993|   3| 1.0|
//        |   A|  XX|1994|   4| 9.0|
//        |   A|  XX|1995|   7| 5.0|
//        |   A|  XX|1996|   6| 8.0|
//        |   A|  YY|1993|   9| 3.0|
//        |   A|  YY|1994|   5| 4.0|
//        |   A|  YY|1995|  10| 2.0|
//        |   A|  YY|1996|   2| 7.0|
//        +----+----+----+----+----+
//        * */
//        ScriptContext context = engine.getContext();
//        context.setAttribute("ds2", anCountDS2, ScriptContext.ENGINE_SCOPE);
//
//
//        engine.eval("res := lead ( ds2 , 1 over ( partition by Id_1 , Id_2 order by Year ) );");
//        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);
//
//        /*
//        *
//        +----+----+----+----+----+---------+---------+
//        |Id_1|Id_2|Year|Me_1|Me_2|lead_Me_1|lead_Me_2|
//        +----+----+----+----+----+---------+---------+
//        |   A|  XX|1993|   3| 1.0|        4|      9.0|
//        |   A|  XX|1994|   4| 9.0|        7|      5.0|
//        |   A|  XX|1995|   7| 5.0|        6|      8.0|
//        |   A|  XX|1996|   6| 8.0|     null|     null|
//        |   A|  YY|1993|   9| 3.0|        5|      4.0|
//        |   A|  YY|1994|   5| 4.0|       10|      2.0|
//        |   A|  YY|1995|  10| 2.0|        2|      7.0|
//        |   A|  YY|1996|   2| 7.0|     null|     null|
//        +----+----+----+----+----+---------+---------+
//        * */
//        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 1993L, "Me_1", 4L, "Me_2", 9.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 1994L, "Me_1", 7L, "Me_2", 5.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 1995L, "Me_1", 6L, "Me_2", 8.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 1996L, "Me_1", null, "Me_2", null),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 1993L, "Me_1", 5L, "Me_2", 4.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 1994L, "Me_1", 10L, "Me_2", 2.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 1995L, "Me_1", 2L, "Me_2", 7.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 1996L, "Me_1", null, "Me_2", null)
//        );
//
//    }
//
//    /*
//     * End of lead test case */
//
//    /*
//     * Test case for analytic function lag
//     * The lag function take two argument:
//     * - input dataframe
//     * - step
//     * Analytic clause restriction:
//     * - Must have orderClause
//     * - The windowClause such as data points and range are not allowed
//     * */
//    @Test
//    public void testAnLag() throws ScriptException {
//
//        // Analytical function Test case 1 : lag on window with partition, order by and range
//        /* Input dataset
//        +----+----+----+----+----+
//        |Id_1|Id_2|Year|Me_1|Me_2|
//        +----+----+----+----+----+
//        |   A|  XX|1993|   3| 1.0|
//        |   A|  XX|1994|   4| 9.0|
//        |   A|  XX|1995|   7| 5.0|
//        |   A|  XX|1996|   6| 8.0|
//        |   A|  YY|1993|   9| 3.0|
//        |   A|  YY|1994|   5| 4.0|
//        |   A|  YY|1995|  10| 2.0|
//        |   A|  YY|1996|   2| 7.0|
//        +----+----+----+----+----+
//        * */
//        ScriptContext context = engine.getContext();
//        context.setAttribute("ds2", anCountDS2, ScriptContext.ENGINE_SCOPE);
//
//
//        engine.eval("res := lag ( ds2 , 1 over ( partition by Id_1 , Id_2 order by Year ) );");
//        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);
//
//        /*
//        *
//        +----+----+----+----+----+---------+---------+
//        |Id_1|Id_2|Year|Me_1|Me_2|lead_Me_1|lead_Me_2|
//        +----+----+----+----+----+---------+---------+
//        |   A|  XX|1993|   3| 1.0|     null|     null|
//        |   A|  XX|1994|   4| 9.0|        3|      1.0|
//        |   A|  XX|1995|   7| 5.0|        4|      9.0|
//        |   A|  XX|1996|   6| 8.0|        7|      5.0|
//        |   A|  YY|1993|   9| 3.0|     null|     null|
//        |   A|  YY|1994|   5| 4.0|        9|      3.0|
//        |   A|  YY|1995|  10| 2.0|        5|      4.0|
//        |   A|  YY|1996|   2| 7.0|       10|      2.0|
//        +----+----+----+----+----+---------+---------+
//        * */
//        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 1993L, "Me_1", null, "Me_2", null),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 1994L, "Me_1", 3L, "Me_2", 1.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 1995L, "Me_1", 4L, "Me_2", 9.0D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 1996L, "Me_1", 7L, "Me_2", 5.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 1993L, "Me_1", null, "Me_2", null),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 1994L, "Me_1", 9L, "Me_2", 3.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 1995L, "Me_1", 5L, "Me_2", 4.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 1996L, "Me_1", 10L, "Me_2", 2.0D)
//        );
//
//    }
//    /*
//     * End of lag test case */
//
//    /*
//     * Test case for analytic function ratio_to_report
//     * Analytic clause restriction:
//     * - The orderClause and windowClause of the Analytic invocation syntax are not allowed.
//     * */
//
//    @Test
//    public void testAnRatioToReport() throws ScriptException {
//
//        InMemoryDataset anDS = new InMemoryDataset(
//                List.of(
//                        Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 3L, "Me_2", 1D),
//                        Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 4L, "Me_2", 3D),
//                        Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 7L, "Me_2", 5D),
//                        Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 6L, "Me_2", 1D),
//                        Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 12L, "Me_2", 0D),
//                        Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 8L, "Me_2", 8D),
//                        Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 6L, "Me_2", 5D),
//                        Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 14L, "Me_2", -3D)
//
//                ),
//                Map.of("Id_1", String.class, "Id_2", String.class, "Year", Long.class, "Me_1", Long.class, "Me_2", Double.class),
//                Map.of("Id_1", Role.IDENTIFIER, "Id_2", Role.IDENTIFIER, "Year", Role.IDENTIFIER, "Me_1", Role.MEASURE, "Me_2", Role.MEASURE)
//        );
//        /* Input dataset
//        +----+----+----+----+----+
//        |Id_1|Id_2|Id_3|Me_1|Me_2|
//        +----+----+----+----+----+
//        |   A|  XX|2000|   3|   1|
//        |   A|  XX|2001|   4|   3|
//        |   A|  XX|2002|   7|   5|
//        |   A|  XX|2003|   6|   1|
//        |   A|  YY|2000|  12|   0|
//        |   A|  YY|2001|   8|   8|
//        |   A|  YY|2002|   6|   5|
//        |   A|  YY|2003|  14|  -3|
//        +----+----+----+----+----+
//        * */
//        ScriptContext context = engine.getContext();
//        context.setAttribute("ds", anDS, ScriptContext.ENGINE_SCOPE);
//
//
//        engine.eval("res := ratio_to_report ( ds over ( partition by Id_1, Id_2 ) );");
//        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);
//
//        /*
//        *
//        +----+----+----+----+----+----------+----------+----------+----------+
//        |Id_1|Id_2|Id_3|Me_1|Me_2|total_Me_1|ratio_Me_1|total_Me_2|ratio_Me_2|
//        +----+----+----+----+----+----------+----------+----------+----------+
//        |   A|  XX|2000|   3|   1|        20|      0.15|        10|       0.1|
//        |   A|  XX|2001|   4|   3|        20|       0.2|        10|       0.3|
//        |   A|  XX|2002|   7|   5|        20|      0.35|        10|       0.5|
//        |   A|  XX|2003|   6|   1|        20|       0.3|        10|       0.1|
//        |   A|  YY|2000|  12|   0|        40|       0.3|        10|       0.0|
//        |   A|  YY|2001|   8|   8|        40|       0.2|        10|       0.8|
//        |   A|  YY|2002|   6|   5|        40|      0.15|        10|       0.5|
//        |   A|  YY|2003|  14|  -3|        40|      0.35|        10|      -0.3|
//        +----+----+----+----+----+----------+----------+----------+----------+
//        * */
//        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 1993L, "Me_1", 0.15D, "Me_2", 0.1D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 1994L, "Me_1", 0.2D, "Me_2", 0.3D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 1995L, "Me_1", 0.35D, "Me_2", 0.5D),
//                Map.of("Id_1", "A", "Id_2", "XX", "Year", 1996L, "Me_1", 0.3D, "Me_2", 0.1D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 1993L, "Me_1", 0.3D, "Me_2", 0.0D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 1994L, "Me_1", 0.2D, "Me_2", 0.8D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 1995L, "Me_1", 0.15D, "Me_2", 0.5D),
//                Map.of("Id_1", "A", "Id_2", "YY", "Year", 1996L, "Me_1", 0.35D, "Me_2", -0.3D)
//        );
//
//    }
//    /*
//     * End of ratio_to_report test case */


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