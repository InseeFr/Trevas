package fr.insee.vtl.spark.processing.engine.analytic;

import fr.insee.vtl.model.Dataset;
import org.junit.jupiter.api.Test;

import javax.script.ScriptContext;
import javax.script.ScriptException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class AnalyticLagTest extends AnalyticTest {

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
        context.setAttribute("ds2", ds2, ScriptContext.ENGINE_SCOPE);
        engine.eval(
                "res := ds2 [ calc lag_Me_1 := lag ( Me_1 , 1 over ( partition by Id_1 , Id_2 order by Year ) )] ;");
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

        List<Map<String, Object>> actualWithNull =
                ((Dataset) engine.getContext().getAttribute("res")).getDataAsMap();

        List<Map<String, Object>> actual = new ArrayList<>();
        for (Map<String, Object> map : actualWithNull) {
            actual.add(replaceNullValues(map, DEFAULT_NULL_STR));
        }
        assertThat(actual)
                .containsExactly(
                        Map.of(
                                "Id_1",
                                "A",
                                "Id_2",
                                "XX",
                                "Year",
                                1993L,
                                "Me_1",
                                3L,
                                "Me_2",
                                1.0D,
                                "lag_Me_1",
                                "null"),
                        Map.of(
                                "Id_1", "A", "Id_2", "XX", "Year", 1994L, "Me_1", 4L, "Me_2", 9.0D, "lag_Me_1", 3L),
                        Map.of(
                                "Id_1", "A", "Id_2", "XX", "Year", 1995L, "Me_1", 7L, "Me_2", 5.0D, "lag_Me_1", 4L),
                        Map.of(
                                "Id_1", "A", "Id_2", "XX", "Year", 1996L, "Me_1", 6L, "Me_2", 8.0D, "lag_Me_1", 7L),
                        Map.of(
                                "Id_1",
                                "A",
                                "Id_2",
                                "YY",
                                "Year",
                                1993L,
                                "Me_1",
                                9L,
                                "Me_2",
                                3.0D,
                                "lag_Me_1",
                                "null"),
                        Map.of(
                                "Id_1", "A", "Id_2", "YY", "Year", 1994L, "Me_1", 5L, "Me_2", 4.0D, "lag_Me_1", 9L),
                        Map.of(
                                "Id_1",
                                "A",
                                "Id_2",
                                "YY",
                                "Year",
                                1995L,
                                "Me_1",
                                10L,
                                "Me_2",
                                2.0D,
                                "lag_Me_1",
                                5L),
                        Map.of(
                                "Id_1",
                                "A",
                                "Id_2",
                                "YY",
                                "Year",
                                1996L,
                                "Me_1",
                                2L,
                                "Me_2",
                                7.0D,
                                "lag_Me_1",
                                10L));
    }

    /*
     * Test case for analytic function lag
     * The lag function take two argument:
     * - input dataframe
     * - step
     * Analytic clause restriction:
     * - Must have orderClause
     * - The windowClause such as data points and range are not allowed
     * */
    @Test
    public void testAnLag() throws ScriptException {

        // Analytical function Test case 1 : lag on window with partition, order by and range
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
        context.setAttribute("ds2", ds2, ScriptContext.ENGINE_SCOPE);

        engine.eval("res := lag ( ds2 , 1 over ( partition by Id_1 , Id_2 order by Year ) );");
        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);

    /*
    *
    +----+----+----+----+----+---------+---------+
    |Id_1|Id_2|Year|Me_1|Me_2|lead_Me_1|lead_Me_2|
    +----+----+----+----+----+---------+---------+
    |   A|  XX|1993|   3| 1.0|     null|     null|
    |   A|  XX|1994|   4| 9.0|        3|      1.0|
    |   A|  XX|1995|   7| 5.0|        4|      9.0|
    |   A|  XX|1996|   6| 8.0|        7|      5.0|
    |   A|  YY|1993|   9| 3.0|     null|     null|
    |   A|  YY|1994|   5| 4.0|        9|      3.0|
    |   A|  YY|1995|  10| 2.0|        5|      4.0|
    |   A|  YY|1996|   2| 7.0|       10|      2.0|
    +----+----+----+----+----+---------+---------+
    * */
        List<Map<String, Object>> actualWithNull =
                ((Dataset) engine.getContext().getAttribute("res")).getDataAsMap();
        var actual =
                actualWithNull.stream()
                        .map(map -> replaceNullValues(map, DEFAULT_NULL_STR))
                        .collect(Collectors.toList());

        assertThat(actual)
                .containsExactly(
                        Map.of("Id_1", "A", "Id_2", "XX", "Year", 1993L, "Me_1", "null", "Me_2", "null"),
                        Map.of("Id_1", "A", "Id_2", "XX", "Year", 1994L, "Me_1", 3L, "Me_2", 1.0D),
                        Map.of("Id_1", "A", "Id_2", "XX", "Year", 1995L, "Me_1", 4L, "Me_2", 9.0D),
                        Map.of("Id_1", "A", "Id_2", "XX", "Year", 1996L, "Me_1", 7L, "Me_2", 5.0D),
                        Map.of("Id_1", "A", "Id_2", "YY", "Year", 1993L, "Me_1", "null", "Me_2", "null"),
                        Map.of("Id_1", "A", "Id_2", "YY", "Year", 1994L, "Me_1", 9L, "Me_2", 3.0D),
                        Map.of("Id_1", "A", "Id_2", "YY", "Year", 1995L, "Me_1", 5L, "Me_2", 4.0D),
                        Map.of("Id_1", "A", "Id_2", "YY", "Year", 1996L, "Me_1", 10L, "Me_2", 2.0D));
    }
}
