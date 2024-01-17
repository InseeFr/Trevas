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

public class AnalyticSumTest {

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
            Map.of("Id_1", Dataset.Role.IDENTIFIER, "Id_2", Dataset.Role.IDENTIFIER, "Year", Dataset.Role.IDENTIFIER, "Me_1", Dataset.Role.MEASURE, "Me_2", Dataset.Role.MEASURE)
    );

    private static SparkSession spark;
    private static ScriptEngine engine;

    @BeforeAll
    public static void setUp() {

        ScriptEngineManager mgr = new ScriptEngineManager();
        engine = mgr.getEngineByExtension("vtl");

        spark = SparkSession.builder()
                .appName("test")
                .master("local")
                .getOrCreate();
        SparkSession.setActiveSession(spark);

        engine.put(VtlScriptEngine.PROCESSING_ENGINE_NAMES, "spark");
    }

    @AfterAll
    public static void tearDown() {
        if (spark != null)
            spark.close();
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
        context.setAttribute("ds1", anCountDS1, ScriptContext.ENGINE_SCOPE);


        engine.eval("res := sum ( ds1 over ( partition by Id_1, Id_2 ) );");
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
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 20L, "Me_2", 23.0D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 20L, "Me_2", 23.0D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 20L, "Me_2", 23.0D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 20L, "Me_2", 23.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 29L, "Me_2", 16.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 29L, "Me_2", 16.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 29L, "Me_2", 16.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 29L, "Me_2", 16.0D)
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
        context.setAttribute("ds1", anCountDS1, ScriptContext.ENGINE_SCOPE);


        engine.eval("res := sum ( ds1 over ( partition by Id_1 order by Id_2) );");
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
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 20L, "Me_2", 23.0D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 20L, "Me_2", 23.0D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 20L, "Me_2", 23.0D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 20L, "Me_2", 23.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 49L, "Me_2", 39.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 49L, "Me_2", 39.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 49L, "Me_2", 39.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 49L, "Me_2", 39.0D)
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
        context.setAttribute("ds1", anCountDS1, ScriptContext.ENGINE_SCOPE);


        engine.eval("res := sum ( ds1 over ( order by Id_1, Id_2, Year ) );");
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
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 3L, "Me_2", 1.0D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 7L, "Me_2", 10.0D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 14L, "Me_2", 15.0D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 20L, "Me_2", 23.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 29L, "Me_2", 26.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 34L, "Me_2", 30.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 44L, "Me_2", 32.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 49L, "Me_2", 39.0D)
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
        context.setAttribute("ds1", anCountDS1, ScriptContext.ENGINE_SCOPE);


        engine.eval("res := sum ( ds1 over ( partition by Id_1 order by Id_2 data points between 2 preceding and 2 following) );");
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
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 14L, "Me_2", 15.0D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 20L, "Me_2", 23.0D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 29L, "Me_2", 26.0D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 31L, "Me_2", 29.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 37L, "Me_2", 22.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 35L, "Me_2", 24.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 29L, "Me_2", 16.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 20L, "Me_2", 13.0D)
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
        context.setAttribute("ds1", anCountDS1, ScriptContext.ENGINE_SCOPE);


        engine.eval("res := sum ( ds1 over ( partition by Id_1 order by Year range between 1 preceding and 1 following) );");
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
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 21L, "Me_2", 17.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 21L, "Me_2", 17.0D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 38L, "Me_2", 24.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 38L, "Me_2", 24.0D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 37L, "Me_2", 35.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 37L, "Me_2", 35.0D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 28L, "Me_2", 22.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 28L, "Me_2", 22.0D)
        );

    }
}
