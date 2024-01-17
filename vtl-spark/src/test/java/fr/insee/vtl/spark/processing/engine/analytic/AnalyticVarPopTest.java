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

public class AnalyticVarPopTest {

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

    /*
     * Test case for analytic function var_pop
     *
     * */
    @Test
    public void testAnVarPopWithPartitionClause() throws ScriptException {

        // Analytical function Test case 1 : var_pop on window with partition
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


        engine.eval("res := var_pop ( ds1 over ( partition by Id_1, Id_2 ) );");
        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);

        /*
        *

            +----+----+----+----+----+------------------+------------+
            |Id_1|Id_2|Year|Me_1|Me_2|      var_pop_Me_1|var_pop_Me_2|
            +----+----+----+----+----+------------------+------------+
            |   A|  XX|2000|   3| 1.0|2.4999999999999996|      9.6875|
            |   A|  XX|2001|   4| 9.0|2.4999999999999996|      9.6875|
            |   A|  XX|2002|   7| 5.0|2.4999999999999996|      9.6875|
            |   A|  XX|2003|   6| 8.0|2.4999999999999996|      9.6875|
            |   A|  YY|2000|   9| 3.0|            5.1875|         3.5|
            |   A|  YY|2001|   5| 4.0|            5.1875|         3.5|
            |   A|  YY|2002|  10| 2.0|            5.1875|         3.5|
            |   A|  YY|2003|   5| 7.0|            5.1875|         3.5|
            +----+----+----+----+----+------------------+------------+
        * */
        List<Map<String, Object>> res = AnalyticTest.roundDecimalInDataset((Dataset) engine.getContext().getAttribute("res"), AnalyticTest.DEFAULT_PRECISION);
        assertThat(res).containsExactly(
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 2.50D, "Me_2", 9.69D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 2.50D, "Me_2", 9.69D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 2.50D, "Me_2", 9.69D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 2.50D, "Me_2", 9.69D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 5.19D, "Me_2", 3.5D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 5.19D, "Me_2", 3.5D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 5.19D, "Me_2", 3.5D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 5.19D, "Me_2", 3.5D)
        );

    }

    @Test
    public void testAnVarPopWithPartitionOrderByClause() throws ScriptException {

        // Analytical function Test case 2 : var_pop on window with partition and order by
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


        engine.eval("res := var_pop ( ds1 over ( partition by Id_1, Id_2 order by Year) );");
        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);

        /*
        +----+----+----+----+----+------------------+------------------+
        |Id_1|Id_2|Year|Me_1|Me_2|      var_pop_Me_1|      var_pop_Me_2|
        +----+----+----+----+----+------------------+------------------+
        |   A|  XX|2000|   3| 1.0|               0.0|               0.0|
        |   A|  XX|2001|   4| 9.0|              0.25|              16.0|
        |   A|  XX|2002|   7| 5.0| 2.888888888888889|10.666666666666666|
        |   A|  XX|2003|   6| 8.0|2.4999999999999996|            9.6875|
        |   A|  YY|2000|   9| 3.0|               0.0|               0.0|
        |   A|  YY|2001|   5| 4.0|               4.0|              0.25|
        |   A|  YY|2002|  10| 2.0| 4.666666666666667|0.6666666666666666|
        |   A|  YY|2003|   5| 7.0|            5.1875|               3.5|
        +----+----+----+----+----+------------------+------------------+

        * */

        List<Map<String, Object>> res = AnalyticTest.roundDecimalInDataset((Dataset) engine.getContext().getAttribute("res"), AnalyticTest.DEFAULT_PRECISION);


        assertThat(res).contains(
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 0.0D, "Me_2", 0.0D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 0.25D, "Me_2", 16.0D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 2.89D, "Me_2", 10.67D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 2.50D, "Me_2", 9.69D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 0.0D, "Me_2", 0.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 4.0D, "Me_2", 0.25D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 4.67D, "Me_2", 0.67D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 5.19D, "Me_2", 3.5D)
        );

    }


    @Test
    public void testAnVarPopWithPartitionOrderByDPClause() throws ScriptException {

        // Analytical function count test case 3 : var_pop on window with partition, orderBy and data points
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

        engine.eval("res := var_pop ( ds1 over ( partition by Id_1 order by Id_2 data points between 2 preceding and 2 following) );");
        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);

        /*
        * The result data frame
        *
        +----+----+----+----+----+------------------+------------------+
        |Id_1|Id_2|Year|Me_1|Me_2|      var_pop_Me_1|      var_pop_Me_2|
        +----+----+----+----+----+------------------+------------------+
        |   A|  XX|2000|   3| 1.0| 2.888888888888889|10.666666666666666|
        |   A|  XX|2001|   4| 9.0|2.4999999999999996|            9.6875|
        |   A|  XX|2002|   7| 5.0|              4.56| 8.959999999999999|
        |   A|  XX|2003|   6| 8.0|              2.96|              5.36|
        |   A|  YY|2000|   9| 3.0|              3.44| 4.239999999999999|
        |   A|  YY|2001|   5| 4.0|               4.4|              5.36|
        |   A|  YY|2002|  10| 2.0|            5.1875|               3.5|
        |   A|  YY|2003|   5| 7.0|5.5555555555555545| 4.222222222222222|
        +----+----+----+----+----+------------------+------------------+


        * */
        List<Map<String, Object>> res = AnalyticTest.roundDecimalInDataset((Dataset) engine.getContext().getAttribute("res"), AnalyticTest.DEFAULT_PRECISION);
        assertThat(res).containsExactly(
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 2.89D, "Me_2", 10.67D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 2.5D, "Me_2", 9.69D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 4.56D, "Me_2", 8.96D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 2.96D, "Me_2", 5.36D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 3.44D, "Me_2", 4.24D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 4.4D, "Me_2", 5.36D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 5.19D, "Me_2", 3.5D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 5.56D, "Me_2", 4.22D)
        );

    }

    @Test
    public void testAnVarPopWithPartitionOrderByRangeClause() throws ScriptException {

        // Analytical function count test case 4 : var_pop on window with partition, orderBy and range
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


        engine.eval("res := var_pop ( ds1 over ( partition by Id_1 order by Year range between 1 preceding and 1 following) );");
        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);

        /*
        * The result data frame
        *
        +----+----+----+----+----+------------------+-----------------+
        |Id_1|Id_2|Year|Me_1|Me_2|      var_pop_Me_1|     var_pop_Me_2|
        +----+----+----+----+----+------------------+-----------------+
        |   A|  XX|2000|   3| 1.0|            5.1875|           8.6875|
        |   A|  YY|2000|   9| 3.0|            5.1875|           8.6875|
        |   A|  XX|2001|   4| 9.0| 6.555555555555556|6.666666666666668|
        |   A|  YY|2001|   5| 4.0| 6.555555555555556|6.666666666666668|
        |   A|  XX|2002|   7| 5.0|3.8055555555555554|5.805555555555556|
        |   A|  YY|2002|  10| 2.0|3.8055555555555554|5.805555555555556|
        |   A|  XX|2003|   6| 8.0|               3.5|             5.25|
        |   A|  YY|2003|   5| 7.0|               3.5|             5.25|
        +----+----+----+----+----+------------------+-----------------+

        * */
        List<Map<String, Object>> res = AnalyticTest.roundDecimalInDataset(
                (Dataset) engine.getContext().getAttribute("res"),
                AnalyticTest.DEFAULT_PRECISION
        );
        assertThat(res).containsExactlyInAnyOrder(
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 5.19D, "Me_2", 8.69D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 5.19D, "Me_2", 8.69D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 6.56D, "Me_2", 6.67D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 6.56D, "Me_2", 6.67D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 3.81D, "Me_2", 5.81D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 3.81D, "Me_2", 5.81D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 3.5D, "Me_2", 5.25D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 3.5D, "Me_2", 5.25D)
        );

    }
}
