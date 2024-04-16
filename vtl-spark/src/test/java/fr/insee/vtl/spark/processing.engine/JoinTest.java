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
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

public class JoinTest {

    private final InMemoryDataset dataset1 = new InMemoryDataset(
            Java8Helpers.listOf(
                    Java8Helpers.listOf("a", 1L, 2L),
                    Java8Helpers.listOf("b", 3L, 4L),
                    Java8Helpers.listOf("c", 5L, 6L),
                    Java8Helpers.listOf("d", 7L, 8L)
            ),
            Java8Helpers.listOf(
                    new Structured.Component("name", String.class, Dataset.Role.IDENTIFIER),
                    new Structured.Component("age", Long.class, Dataset.Role.MEASURE),
                    new Structured.Component("weight", Long.class, Dataset.Role.MEASURE)
            )
    );
    private final InMemoryDataset dataset2 = new InMemoryDataset(
            Java8Helpers.listOf(
                    Java8Helpers.listOf(9L, "a", 10L),
                    Java8Helpers.listOf(11L, "b", 12L),
                    Java8Helpers.listOf(12L, "c", 13L),
                    Java8Helpers.listOf(14L, "c", 15L)
            ),
            Java8Helpers.listOf(
                    new Structured.Component("age2", Long.class, Dataset.Role.MEASURE),
                    new Structured.Component("name", String.class, Dataset.Role.IDENTIFIER),
                    new Structured.Component("weight2", Long.class, Dataset.Role.MEASURE)
            )
    );
    private final InMemoryDataset dataset3 = new InMemoryDataset(
            Java8Helpers.listOf(
                    Java8Helpers.listOf(16L, "a", 17L),
                    Java8Helpers.listOf(18L, "b", 19L),
                    Java8Helpers.listOf(20L, "c", 21L),
                    Java8Helpers.listOf(22L, "c", 23L)
            ),
            Java8Helpers.listOf(
                    new Structured.Component("age3", Long.class, Dataset.Role.MEASURE),
                    new Structured.Component("name", String.class, Dataset.Role.IDENTIFIER),
                    new Structured.Component("weight3", Long.class, Dataset.Role.MEASURE)
            )
    );
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
    public void testLeftJoin() throws ScriptException {

        ScriptContext context = engine.getContext();
        context.getBindings(ScriptContext.ENGINE_SCOPE).put("ds_1", dataset1);
        context.getBindings(ScriptContext.ENGINE_SCOPE).put("ds_2", dataset2);
        context.getBindings(ScriptContext.ENGINE_SCOPE).put("ds_3", dataset3);

        engine.eval("result := left_join(ds_1 as ds1, ds_2 as ds2, ds_3 as ds3);");

        Dataset result = (Dataset) context.getAttribute("result");
        assertThat(result.getDataAsList()).containsExactlyInAnyOrder(
                Arrays.asList("a", 2L, 16L, 17L, 10L, 1L, 9L),
                Arrays.asList("b", 4L, 18L, 19L, 12L, 3L, 11L),
                Arrays.asList("c", 6L, 22L, 23L, 15L, 5L, 14L),
                Arrays.asList("c", 6L, 20L, 21L, 15L, 5L, 14L),
                Arrays.asList("c", 6L, 22L, 23L, 13L, 5L, 12L),
                Arrays.asList("c", 6L, 20L, 21L, 13L, 5L, 12L),
                Arrays.asList("d", 8L, null, null, null, 7L, null)
        );

        assertThat(result.getDataStructure()).containsValues(
                new Structured.Component("name", String.class, Dataset.Role.IDENTIFIER),
                new Structured.Component("age", Long.class, Dataset.Role.MEASURE),
                new Structured.Component("weight", Long.class, Dataset.Role.MEASURE),
                new Structured.Component("age2", Long.class, Dataset.Role.MEASURE),
                new Structured.Component("weight2", Long.class, Dataset.Role.MEASURE),
                new Structured.Component("age3", Long.class, Dataset.Role.MEASURE),
                new Structured.Component("weight3", Long.class, Dataset.Role.MEASURE)
        );
    }

    @Test
    public void testInnerJoin() throws ScriptException {
        ScriptContext context = engine.getContext();
        context.getBindings(ScriptContext.ENGINE_SCOPE).put("ds1", dataset1);
        context.getBindings(ScriptContext.ENGINE_SCOPE).put("ds2", dataset2);
        context.getBindings(ScriptContext.ENGINE_SCOPE).put("ds3", dataset3);

        engine.eval("result := inner_join(ds1 as dsOne, ds2, ds3);");

        Dataset resultInner = (Dataset) context.getAttribute("result");
        assertThat(resultInner.getDataAsList()).containsExactlyInAnyOrder(
                Arrays.asList("a", 2L, 16L, 17L, 10L, 1L, 9L),
                Arrays.asList("b", 4L, 18L, 19L, 12L, 3L, 11L),
                Arrays.asList("c", 6L, 22L, 23L, 15L, 5L, 14L),
                Arrays.asList("c", 6L, 20L, 21L, 15L, 5L, 14L),
                Arrays.asList("c", 6L, 22L, 23L, 13L, 5L, 12L),
                Arrays.asList("c", 6L, 20L, 21L, 13L, 5L, 12L)
        );

        assertThat(resultInner.getDataStructure()).containsValues(
                new Structured.Component("name", String.class, Dataset.Role.IDENTIFIER),
                new Structured.Component("age", Long.class, Dataset.Role.MEASURE),
                new Structured.Component("weight", Long.class, Dataset.Role.MEASURE),
                new Structured.Component("age2", Long.class, Dataset.Role.MEASURE),
                new Structured.Component("weight2", Long.class, Dataset.Role.MEASURE),
                new Structured.Component("age3", Long.class, Dataset.Role.MEASURE),
                new Structured.Component("weight3", Long.class, Dataset.Role.MEASURE)
        );
    }

    @Test
    public void testFullJoin() throws ScriptException {
        ScriptContext context = engine.getContext();

        InMemoryDataset ds1 = new InMemoryDataset(
                Java8Helpers.listOf(
                        new Structured.Component("id", String.class, Dataset.Role.IDENTIFIER),
                        new Structured.Component("m1", Long.class, Dataset.Role.MEASURE)
                ),
                Arrays.asList("b", 1L),
                Arrays.asList("c", 2L),
                Arrays.asList("d", 3L)
        );

        InMemoryDataset ds2 = new InMemoryDataset(
                Java8Helpers.listOf(
                        new Structured.Component("id", String.class, Dataset.Role.IDENTIFIER),
                        new Structured.Component("m1", Long.class, Dataset.Role.MEASURE)
                ),
                Arrays.asList("a", 4L),
                Arrays.asList("b", 5L),
                Arrays.asList("c", 6L)
        );

        InMemoryDataset ds3 = new InMemoryDataset(
                Java8Helpers.listOf(
                        new Structured.Component("id", String.class, Dataset.Role.IDENTIFIER),
                        new Structured.Component("m1", Long.class, Dataset.Role.MEASURE)
                ),
                Arrays.asList("a", 7L),
                Arrays.asList("d", 8L)
        );

        context.getBindings(ScriptContext.ENGINE_SCOPE).put("ds_1", ds1);
        context.getBindings(ScriptContext.ENGINE_SCOPE).put("ds_2", ds2);
        context.getBindings(ScriptContext.ENGINE_SCOPE).put("ds_3", ds3);

        engine.eval("result := full_join(ds_1 as ds1, ds_2 as ds2, ds_3 as ds3);");

        Dataset result = (Dataset) context.getAttribute("result");

        assertThat(result.getDataStructure().values()).containsExactlyInAnyOrder(
                new Structured.Component("id", String.class, Dataset.Role.IDENTIFIER),
                new Structured.Component("m1", Long.class, Dataset.Role.MEASURE)
        );

        assertThat(result.getDataAsList()).containsExactlyInAnyOrder(
                Arrays.asList(8L, "d"),
                Arrays.asList(null, "c"),
                Arrays.asList(null, "b"),
                Arrays.asList(7L, "a")
        );

    }

    @Test
    public void testCrossJoin() throws ScriptException {
        ScriptContext context = engine.getContext();
        context.getBindings(ScriptContext.ENGINE_SCOPE).put("ds1", dataset1);
        context.getBindings(ScriptContext.ENGINE_SCOPE).put("ds2", dataset2);
        context.getBindings(ScriptContext.ENGINE_SCOPE).put("ds3", dataset3);

        engine.eval("result := cross_join(ds1 as dsOne, ds2, ds3);");

        Dataset resultCross = (Dataset) context.getAttribute("result");
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
                new Structured.Component("dsOne#name", String.class, Dataset.Role.IDENTIFIER),
                new Structured.Component("age", Long.class, Dataset.Role.MEASURE),
                new Structured.Component("weight", Long.class, Dataset.Role.MEASURE),
                new Structured.Component("age2", Long.class, Dataset.Role.MEASURE),
                new Structured.Component("ds2#name", String.class, Dataset.Role.IDENTIFIER),
                new Structured.Component("weight2", Long.class, Dataset.Role.MEASURE),
                new Structured.Component("age3", Long.class, Dataset.Role.MEASURE),
                new Structured.Component("ds3#name", String.class, Dataset.Role.IDENTIFIER),
                new Structured.Component("weight3", Long.class, Dataset.Role.MEASURE)
        );
    }
}
