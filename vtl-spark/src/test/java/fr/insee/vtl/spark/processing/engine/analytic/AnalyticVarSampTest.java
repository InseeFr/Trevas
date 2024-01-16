package fr.insee.vtl.spark.processing.engine.analytic;

import fr.insee.vtl.model.Dataset;
import org.junit.jupiter.api.Test;

import javax.script.ScriptContext;
import javax.script.ScriptException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class AnalyticVarSampTest extends AnalyticTest {

    @Test
    public void testAnVarSampWithPartitionClause() throws ScriptException {

        // Analytical function Test case 1 : var_samp on window with partition
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


        engine.eval("res := var_samp ( ds1 over ( partition by Id_1, Id_2 ) );");
        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);

        /*
        *

            +----+----+----+----+----+------------------+------------------+
            |Id_1|Id_2|Year|Me_1|Me_2|     var_samp_Me_1|     var_samp_Me_2|
            +----+----+----+----+----+------------------+------------------+
            |   A|  XX|2000|   3| 1.0|3.3333333333333326|12.916666666666666|
            |   A|  XX|2001|   4| 9.0|3.3333333333333326|12.916666666666666|
            |   A|  XX|2002|   7| 5.0|3.3333333333333326|12.916666666666666|
            |   A|  XX|2003|   6| 8.0|3.3333333333333326|12.916666666666666|
            |   A|  YY|2000|   9| 3.0| 6.916666666666667| 4.666666666666667|
            |   A|  YY|2001|   5| 4.0| 6.916666666666667| 4.666666666666667|
            |   A|  YY|2002|  10| 2.0| 6.916666666666667| 4.666666666666667|
            |   A|  YY|2003|   5| 7.0| 6.916666666666667| 4.666666666666667|
            +----+----+----+----+----+------------------+------------------+
        * */
        List<Map<String, Object>> res = AnalyticTest.roundDecimalInDataset((Dataset) engine.getContext().getAttribute("res"),AnalyticTest.DEFAULT_PRECISION);
        assertThat(res).contains(
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 3.33D, "Me_2", 12.92D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 3.33D, "Me_2", 12.92D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 3.33D, "Me_2", 12.92D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 3.33D, "Me_2", 12.92D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 6.92D, "Me_2", 4.67D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 6.92D, "Me_2", 4.67D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 6.92D, "Me_2", 4.67D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 6.92D, "Me_2", 4.67D)
        );

    }

    @Test
    public void testAnVarSampWithPartitionOrderByClause() throws ScriptException {

        // Analytical function Test case 2 : var_samp on window with partition and order by
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


        engine.eval("res := var_samp ( ds1 over ( partition by Id_1, Id_2 order by Year) );" +
                "res := res [calc Me_1 := round(Me_1, 2), Me_2 := round(Me_2, 2)];");
        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);

        /*
        +----+----+----+----+----+------------------+------------------+
        |Id_1|Id_2|Year|Me_1|Me_2|     var_samp_Me_1|     var_samp_Me_2|
        +----+----+----+----+----+------------------+------------------+
        |   A|  XX|2000|   3| 1.0|              null|              null|
        |   A|  XX|2001|   4| 9.0|               0.5|              32.0|
        |   A|  XX|2002|   7| 5.0| 4.333333333333333|              16.0|
        |   A|  XX|2003|   6| 8.0|3.3333333333333326|12.916666666666666|
        |   A|  YY|2000|   9| 3.0|              null|              null|
        |   A|  YY|2001|   5| 4.0|               8.0|               0.5|
        |   A|  YY|2002|  10| 2.0|               7.0|               1.0|
        |   A|  YY|2003|   5| 7.0| 6.916666666666667| 4.666666666666667|
        +----+----+----+----+----+------------------+------------------+
        * */

        var actual = ((Dataset) engine.getContext().getAttribute("res")).getDataAsMap().stream()
                .map(map -> replaceNullValues(map, DEFAULT_NULL_STR))
                .collect(Collectors.toList());
        assertThat(actual).containsExactly(
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", "null", "Me_2", "null"),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 0.5D, "Me_2", 32.0D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 4.33D, "Me_2", 16.0D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 3.33D, "Me_2", 12.92D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", "null", "Me_2", "null"),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 8.0D, "Me_2", 0.5D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 7.0D, "Me_2", 1.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 6.92D, "Me_2", 4.67D)
        );

    }


    @Test
    public void testAnVarSampWithPartitionOrderByDPClause() throws ScriptException {

        // Analytical function count test case 3 : var_samp on window with partition, orderBy and data points
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

        engine.eval("res := var_samp ( ds1 over ( partition by Id_1 order by Id_2 data points between 2 preceding and 2 following) );");
        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);

        /*
        * The result data frame
        *
        +----+----+----+----+----+------------------+------------------+
        |Id_1|Id_2|Year|Me_1|Me_2|     var_samp_Me_1|     var_samp_Me_2|
        +----+----+----+----+----+------------------+------------------+
        |   A|  XX|2000|   3| 1.0| 4.333333333333333|              16.0|
        |   A|  XX|2001|   4| 9.0|3.3333333333333326|12.916666666666666|
        |   A|  XX|2002|   7| 5.0| 5.699999999999999|              11.2|
        |   A|  XX|2003|   6| 8.0|               3.7|               6.7|
        |   A|  YY|2000|   9| 3.0|               4.3| 5.299999999999999|
        |   A|  YY|2001|   5| 4.0|               5.5|               6.7|
        |   A|  YY|2002|  10| 2.0| 6.916666666666667| 4.666666666666667|
        |   A|  YY|2003|   5| 7.0| 8.333333333333332| 6.333333333333334|
        +----+----+----+----+----+------------------+------------------+



        * */
        List<Map<String, Object>> res = AnalyticTest.roundDecimalInDataset((Dataset) engine.getContext().getAttribute("res"),AnalyticTest.DEFAULT_PRECISION);
        assertThat(res).containsExactly(
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 4.33D, "Me_2", 16.0D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 3.33D, "Me_2", 12.92D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 5.70D, "Me_2", 11.2D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 3.7D, "Me_2", 6.7D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 4.3D, "Me_2", 5.3D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 5.5D, "Me_2", 6.7D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 6.92D, "Me_2", 4.67D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 8.33D, "Me_2", 6.33D)
        );

    }

    @Test
    public void testAnVarSampWithPartitionOrderByRangeClause() throws ScriptException {

        // Analytical function count test case 4 : var_samp on window with partition, orderBy and range
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


        engine.eval("res := var_pop ( ds1 over ( partition by Id_1 order by Year range between 1 preceding and 1 following) );");
        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);

        /*
        * The result data frame
        *
        +----+----+----+----+----+-----------------+------------------+
        |Id_1|Id_2|Year|Me_1|Me_2|    var_samp_Me_1|     var_samp_Me_2|
        +----+----+----+----+----+-----------------+------------------+
        |   A|  XX|2000|   3| 1.0|6.916666666666667|11.583333333333334|
        |   A|  YY|2000|   9| 3.0|6.916666666666667|11.583333333333334|
        |   A|  XX|2001|   4| 9.0|7.866666666666667| 8.000000000000002|
        |   A|  YY|2001|   5| 4.0|7.866666666666667| 8.000000000000002|
        |   A|  XX|2002|   7| 5.0|4.566666666666666| 6.966666666666667|
        |   A|  YY|2002|  10| 2.0|4.566666666666666| 6.966666666666667|
        |   A|  XX|2003|   6| 8.0|4.666666666666667|               7.0|
        |   A|  YY|2003|   5| 7.0|4.666666666666667|               7.0|
        +----+----+----+----+----+-----------------+------------------+

        * */
        List<Map<String, Object>> res = AnalyticTest.roundDecimalInDataset(
                (Dataset) engine.getContext().getAttribute("res"),
                AnalyticTest.DEFAULT_PRECISION
        );
        assertThat(res).containsExactlyInAnyOrder(
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 6.92D, "Me_2", 11.58D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 6.92D, "Me_2", 11.58D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 7.87D, "Me_2", 8.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 7.87D, "Me_2", 8.0D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 4.57D, "Me_2", 6.97D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 4.57D, "Me_2", 6.97D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 4.67D, "Me_2", 7.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 4.67D, "Me_2", 7.0D)
        );

    }
}
