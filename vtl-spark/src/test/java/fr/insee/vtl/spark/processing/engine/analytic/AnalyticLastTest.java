package fr.insee.vtl.spark.processing.engine.analytic;

import fr.insee.vtl.model.Dataset;
import org.junit.jupiter.api.Test;

import javax.script.ScriptContext;
import javax.script.ScriptException;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class AnalyticLastTest extends AnalyticTest {

    /*
     * Test case for analytic function last
     *
     * */
    @Test
    public void testAnLastWithPartitionClause() throws ScriptException {

        // Analytical function Test case 1 : last on window with partition
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


        engine.eval("res :=  last_value ( ds2 over ( partition by Id_1, Id_2) )");
        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);

        /*
        *
        +----+----+----+----+----+---------+---------+
        |Id_1|Id_2|Year|Me_1|Me_2|last_Me_1|last_Me_2|
        +----+----+----+----+----+---------+---------+
        |   A|  XX|1993|   3| 1.0|        6|      8.0|
        |   A|  XX|1994|   4| 9.0|        6|      8.0|
        |   A|  XX|1995|   7| 5.0|        6|      8.0|
        |   A|  XX|1996|   6| 8.0|        6|      8.0|
        |   A|  YY|1993|   9| 3.0|        2|      7.0|
        |   A|  YY|1994|   5| 4.0|        2|      7.0|
        |   A|  YY|1995|  10| 2.0|        2|      7.0|
        |   A|  YY|1996|   2| 7.0|        2|      7.0|
        +----+----+----+----+----+---------+---------+
        * */
        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 1993L, "Me_1", 6L, "Me_2", 8.0D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 1994L, "Me_1", 6L, "Me_2", 8.0D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 1995L, "Me_1", 6L, "Me_2", 8.0D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 1996L, "Me_1", 6L, "Me_2", 8.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 1993L, "Me_1", 2L, "Me_2", 7.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 1994L, "Me_1", 2L, "Me_2", 7.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 1995L, "Me_1", 2L, "Me_2", 7.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 1996L, "Me_1", 2L, "Me_2", 7.0D)
        );

    }

    @Test
    public void testAnLastPartitionOrderByDesc() throws ScriptException {

        // Analytical function Test case 2 : last on window with partition and desc order
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


        engine.eval("res :=  last_value ( ds2 over ( partition by Id_1, Id_2 order by Year desc) );");
        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);

        /*
        *
        +----+----+----+----+----+---------+---------+
        |Id_1|Id_2|Year|Me_1|Me_2|last_Me_1|last_Me_2|
        +----+----+----+----+----+---------+---------+
        |   A|  XX|1996|   6| 8.0|        6|      8.0|
        |   A|  XX|1995|   7| 5.0|        7|      5.0|
        |   A|  XX|1994|   4| 9.0|        4|      9.0|
        |   A|  XX|1993|   3| 1.0|        3|      1.0|
        |   A|  YY|1996|   2| 7.0|        2|      7.0|
        |   A|  YY|1995|  10| 2.0|       10|      2.0|
        |   A|  YY|1994|   5| 4.0|        5|      4.0|
        |   A|  YY|1993|   9| 3.0|        9|      3.0|
        +----+----+----+----+----+---------+---------+
        * */
        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 1996L, "Me_1", 6L, "Me_2", 8.0D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 1995L, "Me_1", 7L, "Me_2", 5.0D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 1994L, "Me_1", 4L, "Me_2", 9.0D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 1993L, "Me_1", 3L, "Me_2", 1.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 1996L, "Me_1", 2L, "Me_2", 7.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 1995L, "Me_1", 10L, "Me_2", 2.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 1994L, "Me_1", 5L, "Me_2", 4.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 1993L, "Me_1", 9L, "Me_2", 3.0D)
        );

    }

    @Test
    public void testAnLastWithPartitionOrderByDPClause() throws ScriptException {

        // Analytical function Test case 3 : last on window with partition, order by and data points
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


        engine.eval("res := last_value ( ds2 over ( partition by Id_1 order by Id_2 data points between 2 preceding and 2 following) );");
        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);

        /*
        *
        +----+----+----+----+----+---------+---------+
        |Id_1|Id_2|Year|Me_1|Me_2|last_Me_1|last_Me_2|
        +----+----+----+----+----+---------+---------+
        |   A|  XX|1993|   3| 1.0|        7|      5.0|
        |   A|  XX|1994|   4| 9.0|        6|      8.0|
        |   A|  XX|1995|   7| 5.0|        9|      3.0|
        |   A|  XX|1996|   6| 8.0|        5|      4.0|
        |   A|  YY|1993|   9| 3.0|       10|      2.0|
        |   A|  YY|1994|   5| 4.0|        2|      7.0|
        |   A|  YY|1995|  10| 2.0|        2|      7.0|
        |   A|  YY|1996|   2| 7.0|        2|      7.0|
        +----+----+----+----+----+---------+---------+
        * */
        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 1993L, "Me_1", 7L, "Me_2", 5.0D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 1994L, "Me_1", 6L, "Me_2", 8.0D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 1995L, "Me_1", 9L, "Me_2", 3.0D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 1996L, "Me_1", 5L, "Me_2", 4.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 1993L, "Me_1", 10L, "Me_2", 2.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 1994L, "Me_1", 2L, "Me_2", 7.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 1995L, "Me_1", 2L, "Me_2", 7.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 1996L, "Me_1", 2L, "Me_2", 7.0D)
        );

    }

    @Test
    public void testAnLastPartitionOrderByRangeClause() throws ScriptException {

        // Analytical function Test case 4 : last on window with partition, order by and range
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


        engine.eval("res := last_value ( ds1 over ( partition by Id_1, Id_2 order by Year range between -1 and 1) );");
        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);

        /*
        *
        +----+----+----+----+----+---------+---------+
        |Id_1|Id_2|Year|Me_1|Me_2|last_Me_1|last_Me_2|
        +----+----+----+----+----+---------+---------+
        |   A|  XX|1993|   3| 1.0|        4|      9.0|
        |   A|  XX|1994|   4| 9.0|        7|      5.0|
        |   A|  XX|1995|   7| 5.0|        6|      8.0|
        |   A|  XX|1996|   6| 8.0|        6|      8.0|
        |   A|  YY|1993|   9| 3.0|        5|      4.0|
        |   A|  YY|1994|   5| 4.0|       10|      2.0|
        |   A|  YY|1995|  10| 2.0|        2|      7.0|
        |   A|  YY|1996|   2| 7.0|        2|      7.0|
        +----+----+----+----+----+---------+---------+
        * */
        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 1993L, "Me_1", 4L, "Me_2", 9.0D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 1994L, "Me_1", 7L, "Me_2", 5.0D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 1995L, "Me_1", 6L, "Me_2", 8.0D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 1996L, "Me_1", 6L, "Me_2", 8.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 1993L, "Me_1", 5L, "Me_2", 4.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 1994L, "Me_1", 10L, "Me_2", 2.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 1995L, "Me_1", 2L, "Me_2", 7.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 1996L, "Me_1", 2L, "Me_2", 7.0D)
        );

    }

}
