package fr.insee.vtl.spark.processing.engine.analytic;

import fr.insee.vtl.model.Dataset;
import org.junit.jupiter.api.Test;

import javax.script.ScriptContext;
import javax.script.ScriptException;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class AnalyticStdPopTest extends AnalyticTest {

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
        context.setAttribute("ds1", ds1, ScriptContext.ENGINE_SCOPE);

        engine.eval(
                "res := ds1 [ calc stddev_pop_Me_1:= stddev_pop ( Me_1 over ( partition by Id_1,Id_2 order by Year) )];");
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
        List<Map<String, Object>> actual =
                ((Dataset) engine.getContext().getAttribute("res")).getDataAsMap();

        assertThat(actual)
                .containsExactly(
                        Map.of(
                                "Id_1",
                                "A",
                                "Id_2",
                                "XX",
                                "Year",
                                2000L,
                                "Me_1",
                                3L,
                                "Me_2",
                                1.0D,
                                "stddev_pop_Me_1",
                                0.0D),
                        Map.of(
                                "Id_1",
                                "A",
                                "Id_2",
                                "XX",
                                "Year",
                                2001L,
                                "Me_1",
                                4L,
                                "Me_2",
                                9.0D,
                                "stddev_pop_Me_1",
                                0.5D),
                        Map.of(
                                "Id_1",
                                "A",
                                "Id_2",
                                "XX",
                                "Year",
                                2002L,
                                "Me_1",
                                7L,
                                "Me_2",
                                5.0D,
                                "stddev_pop_Me_1",
                                1.699673171197595D),
                        Map.of(
                                "Id_1",
                                "A",
                                "Id_2",
                                "XX",
                                "Year",
                                2003L,
                                "Me_1",
                                6L,
                                "Me_2",
                                8.0D,
                                "stddev_pop_Me_1",
                                1.5811388300841895D),
                        Map.of(
                                "Id_1",
                                "A",
                                "Id_2",
                                "YY",
                                "Year",
                                2000L,
                                "Me_1",
                                9L,
                                "Me_2",
                                3.0D,
                                "stddev_pop_Me_1",
                                0.0D),
                        Map.of(
                                "Id_1",
                                "A",
                                "Id_2",
                                "YY",
                                "Year",
                                2001L,
                                "Me_1",
                                5L,
                                "Me_2",
                                4.0D,
                                "stddev_pop_Me_1",
                                2.0D),
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
                                "stddev_pop_Me_1",
                                2.160246899469287D),
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
                                "stddev_pop_Me_1",
                                2.277608394786075D));
    }

    @Test
    public void testAnStdPopWithPartitionClause() throws ScriptException {

        // Analytical function Test case 1 : stddev_pop on window with partition
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
        context.setAttribute("ds1", ds1, ScriptContext.ENGINE_SCOPE);

        engine.eval("res := stddev_pop ( ds1 over ( partition by Id_1, Id_2 ) );");
        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);

    /*
    *

        +----+----+----+----+----+------------------+------------------+
        |Id_1|Id_2|Year|Me_1|Me_2|      std_pop_Me_1|      std_pop_Me_2|
        +----+----+----+----+----+------------------+------------------+
        |   A|  XX|2000|   3| 1.0|1.5811388300841895| 3.112474899497183|
        |   A|  XX|2001|   4| 9.0|1.5811388300841895| 3.112474899497183|
        |   A|  XX|2002|   7| 5.0|1.5811388300841895| 3.112474899497183|
        |   A|  XX|2003|   6| 8.0|1.5811388300841895| 3.112474899497183|
        |   A|  YY|2000|   9| 3.0| 2.277608394786075|1.8708286933869707|
        |   A|  YY|2001|   5| 4.0| 2.277608394786075|1.8708286933869707|
        |   A|  YY|2002|  10| 2.0| 2.277608394786075|1.8708286933869707|
        |   A|  YY|2003|   5| 7.0| 2.277608394786075|1.8708286933869707|
        +----+----+----+----+----+------------------+------------------+
    * */
        List<Map<String, Object>> res =
                AnalyticTest.roundDecimalInDataset(
                        (Dataset) engine.getContext().getAttribute("res"), AnalyticTest.DEFAULT_PRECISION);
        assertThat(res)
                .containsExactly(
                        Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 1.58D, "Me_2", 3.11D),
                        Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 1.58D, "Me_2", 3.11D),
                        Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 1.58D, "Me_2", 3.11D),
                        Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 1.58D, "Me_2", 3.11D),
                        Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 2.28D, "Me_2", 1.87D),
                        Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 2.28D, "Me_2", 1.87D),
                        Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 2.28D, "Me_2", 1.87D),
                        Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 2.28D, "Me_2", 1.87D));
    }

    @Test
    public void testAnStdPopWithPartitionOrderByClause() throws ScriptException {

        // Analytical function Test case 2 : stddev_pop on window with partition and order by
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
        context.setAttribute("ds1", ds1, ScriptContext.ENGINE_SCOPE);

        engine.eval("res := stddev_pop ( ds1 over ( partition by Id_1, Id_2 order by Year) );");
        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);

    /*
    +----+----+----+----+----+------------------+------------------+
    |Id_1|Id_2|Year|Me_1|Me_2|      std_pop_Me_1|      std_pop_Me_2|
    +----+----+----+----+----+------------------+------------------+
    |   A|  XX|2000|   3| 1.0|               0.0|               0.0|
    |   A|  XX|2001|   4| 9.0|               0.5|               4.0|
    |   A|  XX|2002|   7| 5.0| 1.699673171197595| 3.265986323710904|
    |   A|  XX|2003|   6| 8.0|1.5811388300841895| 3.112474899497183|
    |   A|  YY|2000|   9| 3.0|               0.0|               0.0|
    |   A|  YY|2001|   5| 4.0|               2.0|               0.5|
    |   A|  YY|2002|  10| 2.0| 2.160246899469287| 0.816496580927726|
    |   A|  YY|2003|   5| 7.0| 2.277608394786075|1.8708286933869707|
    +----+----+----+----+----+------------------+------------------+

    * */
        List<Map<String, Object>> res =
                AnalyticTest.roundDecimalInDataset(
                        (Dataset) engine.getContext().getAttribute("res"), AnalyticTest.DEFAULT_PRECISION);
        assertThat(res)
                .containsExactly(
                        Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 0.0D, "Me_2", 0.0D),
                        Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 0.5D, "Me_2", 4.0D),
                        Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 1.70D, "Me_2", 3.27D),
                        Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 1.58D, "Me_2", 3.11D),
                        Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 0.0D, "Me_2", 0.0D),
                        Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 2.0D, "Me_2", 0.5D),
                        Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 2.16D, "Me_2", 0.82D),
                        Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 2.28D, "Me_2", 1.87D));
    }

    @Test
    public void testAnStdPopWithPartitionOrderByDPClause() throws ScriptException {

        // Analytical function count test case 3 : stddev_pop on window with partition, orderBy and data
        // points
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
        context.setAttribute("ds1", ds1, ScriptContext.ENGINE_SCOPE);

        engine.eval(
                "res := stddev_pop ( ds1 over ( partition by Id_1 order by Id_2 data points between 2 preceding and 2 following) );");
        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);

    /*
    * The result data frame
    *
    +----+----+----+----+----+------------------+------------------+
    |Id_1|Id_2|Year|Me_1|Me_2|      std_pop_Me_1|      std_pop_Me_2|
    +----+----+----+----+----+------------------+------------------+
    |   A|  XX|2000|   3| 1.0| 1.699673171197595| 3.265986323710904|
    |   A|  XX|2001|   4| 9.0|1.5811388300841895| 3.112474899497183|
    |   A|  XX|2002|   7| 5.0|2.1354156504062622| 2.993325909419153|
    |   A|  XX|2003|   6| 8.0|1.7204650534085253|2.3151673805580453|
    |   A|  YY|2000|   9| 3.0|1.8547236990991407|   2.0591260281974|
    |   A|  YY|2001|   5| 4.0|2.0976176963403033|2.3151673805580453|
    |   A|  YY|2002|  10| 2.0| 2.277608394786075|1.8708286933869707|
    |   A|  YY|2003|   5| 7.0| 2.357022603955158|2.0548046676563256|
    +----+----+----+----+----+------------------+------------------+

    * */
        List<Map<String, Object>> res =
                AnalyticTest.roundDecimalInDataset(
                        (Dataset) engine.getContext().getAttribute("res"), AnalyticTest.DEFAULT_PRECISION);
        assertThat(res)
                .containsExactly(
                        Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 1.70D, "Me_2", 3.27D),
                        Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 1.58D, "Me_2", 3.11D),
                        Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 2.14D, "Me_2", 2.99D),
                        Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 1.72D, "Me_2", 2.32D),
                        Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 1.85D, "Me_2", 2.06D),
                        Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 2.1D, "Me_2", 2.32D),
                        Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 2.28D, "Me_2", 1.87D),
                        Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 2.36D, "Me_2", 2.05D));
    }

    @Test
    public void testAnStdPopWithPartitionOrderByRangeClause() throws ScriptException {

        // Analytical function count test case 4 : stddev_pop on window with partition, orderBy and
        // range
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
        context.setAttribute("ds1", ds1, ScriptContext.ENGINE_SCOPE);

        engine.eval(
                "res := stddev_pop ( ds1 over ( partition by Id_1 order by Year range between 1 preceding and 1 following) );");
        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);

    /*
    * The result data frame
    *
    +----+----+----+----+----+------------------+------------------+
    |Id_1|Id_2|Year|Me_1|Me_2|      std_pop_Me_1|      std_pop_Me_2|
    +----+----+----+----+----+------------------+------------------+
    |   A|  XX|2000|   3| 1.0| 2.277608394786075| 2.947456530637899|
    |   A|  YY|2000|   9| 3.0| 2.277608394786075| 2.947456530637899|
    |   A|  XX|2001|   4| 9.0| 2.560381915956203|2.5819888974716116|
    |   A|  YY|2001|   5| 4.0| 2.560381915956203|2.5819888974716116|
    |   A|  XX|2002|   7| 5.0|1.9507833184532708|2.4094720491334933|
    |   A|  YY|2002|  10| 2.0|1.9507833184532708|2.4094720491334933|
    |   A|  XX|2003|   6| 8.0|1.8708286933869707|  2.29128784747792|
    |   A|  YY|2003|   5| 7.0|1.8708286933869707|  2.29128784747792|
    +----+----+----+----+----+------------------+------------------+

    * */
        List<Map<String, Object>> res =
                AnalyticTest.roundDecimalInDataset(
                        (Dataset) engine.getContext().getAttribute("res"), AnalyticTest.DEFAULT_PRECISION);
        assertThat(res)
                .containsExactly(
                        Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 2.28D, "Me_2", 2.95D),
                        Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 2.28D, "Me_2", 2.95D),
                        Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 2.56D, "Me_2", 2.58D),
                        Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 2.56D, "Me_2", 2.58D),
                        Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 1.95D, "Me_2", 2.41D),
                        Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 1.95D, "Me_2", 2.41D),
                        Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 1.87D, "Me_2", 2.29D),
                        Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 1.87D, "Me_2", 2.29D));
    }
}
