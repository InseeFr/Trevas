package fr.insee.vtl.spark.processing.engine.analytic;

import fr.insee.vtl.engine.VtlScriptEngine;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.InMemoryDataset;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class AnalyticMinTest {

    private final InMemoryDataset anCountDS1 =
            new InMemoryDataset(
                    List.of(
                            Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 3L, "Me_2", 1D),
                            Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 4L, "Me_2", 9D),
                            Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 7L, "Me_2", 5D),
                            Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 6L, "Me_2", 8D),
                            Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 9L, "Me_2", 3D),
                            Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 5L, "Me_2", 4D),
                            Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 10L, "Me_2", 2D),
                            Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 5L, "Me_2", 7D)),
                    Map.of(
                            "Id_1",
                            String.class,
                            "Id_2",
                            String.class,
                            "Year",
                            Long.class,
                            "Me_1",
                            Long.class,
                            "Me_2",
                            Double.class),
                    Map.of(
                            "Id_1",
                            Dataset.Role.IDENTIFIER,
                            "Id_2",
                            Dataset.Role.IDENTIFIER,
                            "Year",
                            Dataset.Role.IDENTIFIER,
                            "Me_1",
                            Dataset.Role.MEASURE,
                            "Me_2",
                            Dataset.Role.MEASURE));

    private static SparkSession spark;
    private static ScriptEngine engine;

    @BeforeAll
    public static void setUp() {

        ScriptEngineManager mgr = new ScriptEngineManager();
        engine = mgr.getEngineByExtension("vtl");

        spark = SparkSession.builder().appName("test").master("local").getOrCreate();
        SparkSession.setActiveSession(spark);

        engine.put(VtlScriptEngine.PROCESSING_ENGINE_NAMES, "spark");
    }

    @AfterAll
    public static void tearDown() {
        if (spark != null) spark.close();
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

        engine.eval(
                "res := ds1 [ calc min_Me_1:= min ( Me_1 over ( partition by Id_1,Id_2 order by Year) )];");
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
        List<Map<String, Object>> actual =
                ((Dataset) engine.getContext().getAttribute("res")).getDataAsMap();

        assertThat(actual)
                .containsExactly(
                        Map.of(
                                "Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 3L, "Me_2", 1.0D, "min_Me_1", 3L),
                        Map.of(
                                "Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 4L, "Me_2", 9.0D, "min_Me_1", 3L),
                        Map.of(
                                "Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 7L, "Me_2", 5.0D, "min_Me_1", 3L),
                        Map.of(
                                "Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 6L, "Me_2", 8.0D, "min_Me_1", 3L),
                        Map.of(
                                "Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 9L, "Me_2", 3.0D, "min_Me_1", 9L),
                        Map.of(
                                "Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 5L, "Me_2", 4.0D, "min_Me_1", 5L),
                        Map.of(
                                "Id_1",
                                "A",
                                "Id_2",
                                "YY",
                                "Year",
                                2002L,
                                "Me_1",
                                10L,
                                "Me_2",
                                2.0D,
                                "min_Me_1",
                                5L),
                        Map.of(
                                "Id_1",
                                "A",
                                "Id_2",
                                "YY",
                                "Year",
                                2003L,
                                "Me_1",
                                5L,
                                "Me_2",
                                7.0D,
                                "min_Me_1",
                                5L));
    }

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
        context.setAttribute("ds1", anCountDS1, ScriptContext.ENGINE_SCOPE);

        engine.eval("res := min ( ds1 over ( partition by Id_1, Id_2 ) );");
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
        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap())
                .containsExactly(
                        Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 3L, "Me_2", 1.0D),
                        Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 3L, "Me_2", 1.0D),
                        Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 3L, "Me_2", 1.0D),
                        Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 3L, "Me_2", 1.0D),
                        Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 5L, "Me_2", 2.0D),
                        Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 5L, "Me_2", 2.0D),
                        Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 5L, "Me_2", 2.0D),
                        Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 5L, "Me_2", 2.0D));
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
        context.setAttribute("ds1", anCountDS1, ScriptContext.ENGINE_SCOPE);

        engine.eval("res := min ( ds1 over ( partition by Id_1, Id_2 order by Year) );");
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
        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap())
                .containsExactly(
                        Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 3L, "Me_2", 1.0D),
                        Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 3L, "Me_2", 1.0D),
                        Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 3L, "Me_2", 1.0D),
                        Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 3L, "Me_2", 1.0D),
                        Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 9L, "Me_2", 3.0D),
                        Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 5L, "Me_2", 3.0D),
                        Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 5L, "Me_2", 2.0D),
                        Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 5L, "Me_2", 2.0D));
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
        context.setAttribute("ds1", anCountDS1, ScriptContext.ENGINE_SCOPE);

        engine.eval("res := min ( ds1 over ( order by Id_1, Id_2, Year ) );");
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
        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap())
                .containsExactly(
                        Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 3L, "Me_2", 1.0D),
                        Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 3L, "Me_2", 1.0D),
                        Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 3L, "Me_2", 1.0D),
                        Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 3L, "Me_2", 1.0D),
                        Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 3L, "Me_2", 1.0D),
                        Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 3L, "Me_2", 1.0D),
                        Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 3L, "Me_2", 1.0D),
                        Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 3L, "Me_2", 1.0D));
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
        context.setAttribute("ds1", anCountDS1, ScriptContext.ENGINE_SCOPE);

        engine.eval(
                "res := min ( ds1 over ( partition by Id_1 order by Id_2 data points between 2 preceding and 2 following) );");
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
        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap())
                .containsExactly(
                        Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 3L, "Me_2", 1.0D),
                        Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 3L, "Me_2", 1.0D),
                        Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 3L, "Me_2", 1.0D),
                        Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 4L, "Me_2", 3.0D),
                        Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 5L, "Me_2", 2.0D),
                        Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 5L, "Me_2", 2.0D),
                        Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 5L, "Me_2", 2.0D),
                        Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 5L, "Me_2", 2.0D));
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
        context.setAttribute("ds1", anCountDS1, ScriptContext.ENGINE_SCOPE);

        engine.eval(
                "res := min ( ds1 over ( partition by Id_1 order by Year range between 1 preceding and 1 following) );");
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
        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap())
                .containsExactlyInAnyOrder(
                        Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 3L, "Me_2", 1.0D),
                        Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 3L, "Me_2", 1.0D),
                        Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 3L, "Me_2", 1.0D),
                        Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 3L, "Me_2", 1.0D),
                        Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 4L, "Me_2", 2.0D),
                        Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 4L, "Me_2", 2.0D),
                        Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 5L, "Me_2", 2.0D),
                        Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 5L, "Me_2", 2.0D));
    }
}
