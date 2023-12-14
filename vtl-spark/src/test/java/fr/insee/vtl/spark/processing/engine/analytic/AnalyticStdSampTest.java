package fr.insee.vtl.spark.processing.engine.analytic;

import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.spark.processing.engine.TestUtilities;
import org.junit.jupiter.api.Test;

import javax.script.ScriptContext;
import javax.script.ScriptException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class AnalyticStdSampTest extends AnalyticTest {


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
        context.setAttribute("ds1", ds1, ScriptContext.ENGINE_SCOPE);


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
        for (Map<String, Object> map : actualWithNull) {
            actual.add(replaceNullValues(map, DEFAULT_NULL_STR));
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
    public void testAnStdSampWithPartitionClause() throws ScriptException {

        // Analytical function Test case 1 : stddev_samp on window with partition
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


        engine.eval("res := stddev_samp ( ds1 over ( partition by Id_1, Id_2 ) );");
        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);

        /*
        *

           +----+----+----+----+----+------------------+-----------------+
            |Id_1|Id_2|Year|Me_1|Me_2|     std_samp_Me_1|    std_samp_Me_2|
            +----+----+----+----+----+------------------+-----------------+
            |   A|  XX|2000|   3| 1.0|1.8257418583505536|3.593976442141304|
            |   A|  XX|2001|   4| 9.0|1.8257418583505536|3.593976442141304|
            |   A|  XX|2002|   7| 5.0|1.8257418583505536|3.593976442141304|
            |   A|  XX|2003|   6| 8.0|1.8257418583505536|3.593976442141304|
            |   A|  YY|2000|   9| 3.0|2.6299556396765835|2.160246899469287|
            |   A|  YY|2001|   5| 4.0|2.6299556396765835|2.160246899469287|
            |   A|  YY|2002|  10| 2.0|2.6299556396765835|2.160246899469287|
            |   A|  YY|2003|   5| 7.0|2.6299556396765835|2.160246899469287|
            +----+----+----+----+----+------------------+-----------------+
        * */
        List<Map<String, Object>> res = TestUtilities.roundDecimalInDataset((Dataset) engine.getContext().getAttribute("res"));
        assertThat(res).containsExactly(
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 1.83D, "Me_2", 3.59D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 1.83D, "Me_2", 3.59D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 1.83D, "Me_2", 3.59D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 1.83D, "Me_2", 3.59D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 2.63D, "Me_2", 2.16D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 2.63D, "Me_2", 2.16D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 2.63D, "Me_2", 2.16D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 2.63D, "Me_2", 2.16D)
        );

    }

    @Test
    public void testAnStdSampWithPartitionOrderByClause() throws ScriptException {

        // Analytical function Test case 2 : stddev_samp on window with partition and order by
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


        engine.eval("res := stddev_samp ( ds1 over ( partition by Id_1, Id_2 order by Year) );");
        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);

        /*
        +----+----+----+----+----+------------------+------------------+
        |Id_1|Id_2|Year|Me_1|Me_2|     std_samp_Me_1|     std_samp_Me_2|
        +----+----+----+----+----+------------------+------------------+
        |   A|  XX|2000|   3| 1.0|              null|              null|
        |   A|  XX|2001|   4| 9.0|0.7071067811865476| 5.656854249492381|
        |   A|  XX|2002|   7| 5.0|2.0816659994661326|               4.0|
        |   A|  XX|2003|   6| 8.0|1.8257418583505536| 3.593976442141304|
        |   A|  YY|2000|   9| 3.0|              null|              null|
        |   A|  YY|2001|   5| 4.0|2.8284271247461903|0.7071067811865476|
        |   A|  YY|2002|  10| 2.0|2.6457513110645907|               1.0|
        |   A|  YY|2003|   5| 7.0|2.6299556396765835| 2.160246899469287|
        +----+----+----+----+----+------------------+------------------+

        * */

        /*
         * todo
         * Map.of can't contain null key or value
         *
         * need another way to create data frame
         * */
        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", null, "Me_2", null),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 0.71D, "Me_2", 5.66D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 2.08D, "Me_2", 4.0D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 1.82D, "Me_2", 3.59D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", null, "Me_2", null),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 2.83D, "Me_2", 0.71D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 2.65D, "Me_2", 1.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 2.63D, "Me_2", 2.16D)
        );

    }


    @Test
    public void testAnStdSampWithPartitionOrderByDPClause() throws ScriptException {

        // Analytical function count test case 3 : stddev_samp on window with partition, orderBy and data points
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

        engine.eval("res := stddev_samp ( ds1 over ( partition by Id_1 order by Id_2 data points between 2 preceding and 2 following) );");
        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);

        /*
        * The result data frame
        *
        +----+----+----+----+----+------------------+------------------+
        |Id_1|Id_2|Year|Me_1|Me_2|     std_samp_Me_1|     std_samp_Me_2|
        +----+----+----+----+----+------------------+------------------+
        |   A|  XX|2000|   3| 1.0|2.0816659994661326|               4.0|
        |   A|  XX|2001|   4| 9.0|1.8257418583505536| 3.593976442141304|
        |   A|  XX|2002|   7| 5.0|2.3874672772626644|3.3466401061363023|
        |   A|  XX|2003|   6| 8.0|1.9235384061671346| 2.588435821108957|
        |   A|  YY|2000|   9| 3.0| 2.073644135332772|2.3021728866442674|
        |   A|  YY|2001|   5| 4.0| 2.345207879911715| 2.588435821108957|
        |   A|  YY|2002|  10| 2.0|2.6299556396765835| 2.160246899469287|
        |   A|  YY|2003|   5| 7.0|2.8867513459481287|2.5166114784235836|
        +----+----+----+----+----+------------------+------------------+


        * */

        List<Map<String, Object>> res = TestUtilities.roundDecimalInDataset((Dataset) engine.getContext().getAttribute("res"));

        assertThat(res).contains(
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 2.08D, "Me_2", 4.0D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 1.83D, "Me_2", 3.59D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 2.39D, "Me_2", 3.35D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 1.92D, "Me_2", 2.59D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 2.07D, "Me_2", 2.30D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 2.35D, "Me_2", 2.59D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 2.63D, "Me_2", 2.16D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 2.89D, "Me_2", 2.52D)
        );

    }

    @Test
    public void testAnStdSampWithPartitionOrderByRangeClause() throws ScriptException {

        // Analytical function count test case 4 : stddev_samp on window with partition, orderBy and range
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


        engine.eval("res := stddev_samp ( ds1 over ( partition by Id_1 order by Year range between -1 preceding and 1 following) );");
        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);

        /*
        * The result data frame
        *
        +----+----+----+----+----+------------------+------------------+
        |Id_1|Id_2|Year|Me_1|Me_2|     std_samp_Me_1|     std_samp_Me_2|
        +----+----+----+----+----+------------------+------------------+
        |   A|  XX|2000|   3| 1.0|2.6299556396765835|3.4034296427770228|
        |   A|  YY|2000|   9| 3.0|2.6299556396765835|3.4034296427770228|
        |   A|  XX|2001|   4| 9.0|2.8047578623950176|2.8284271247461903|
        |   A|  YY|2001|   5| 4.0|2.8047578623950176|2.8284271247461903|
        |   A|  XX|2002|   7| 5.0| 2.136976056643281|2.6394443859772205|
        |   A|  YY|2002|  10| 2.0| 2.136976056643281|2.6394443859772205|
        |   A|  XX|2003|   6| 8.0| 2.160246899469287|2.6457513110645907|
        |   A|  YY|2003|   5| 7.0| 2.160246899469287|2.6457513110645907|
        +----+----+----+----+----+------------------+------------------+

        * */
        //todo result wrong, need to recheck the logic with spark
        List<Map<String, Object>> res = TestUtilities.roundDecimalInDataset((Dataset) engine.getContext().getAttribute("res"));
        assertThat(res).contains(
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 2.63D, "Me_2", 3.40D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 2.63D, "Me_2", 3.40D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 2.80D, "Me_2", 2.83D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 2.80D, "Me_2", 2.83D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 2.14D, "Me_2", 2.64D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 2.14D, "Me_2", 2.64D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 2.16D, "Me_2", 2.65D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 2.16D, "Me_2", 2.65D)
        );

    }

}
