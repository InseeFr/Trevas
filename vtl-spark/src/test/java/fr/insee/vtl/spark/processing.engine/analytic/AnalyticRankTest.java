package fr.insee.vtl.spark.processing.engine.analytic;

import fr.insee.vtl.model.Dataset;
import org.junit.jupiter.api.Test;

import javax.script.ScriptContext;
import javax.script.ScriptException;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class AnalyticRankTest extends AnalyticTest {

    /*
     * Test case for analytic function rank
     *  ** rank analytic clause restriction**
     *  - Must have `orderClause`
     * - The `windowClause` such as `data points` and `range` are not allowed
     * */
    @Test
    public void testAnRankAscClause() throws ScriptException {

        // Analytical function Test case 1 : rank on window with partition and asc order
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
                "res := ds2 [calc rank_col := rank ( over ( partition by Id_1, Id_2 order by Year) )];");
        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);

    /*
    *
    +----+----+----+----+----+--------+
    |Id_1|Id_2|Year|Me_1|Me_2|rank_col|
    +----+----+----+----+----+--------+
    |   A|  XX|1993|   3| 1.0|       1|
    |   A|  XX|1994|   4| 9.0|       2|
    |   A|  XX|1995|   7| 5.0|       3|
    |   A|  XX|1996|   6| 8.0|       4|
    |   A|  YY|1993|   9| 3.0|       1|
    |   A|  YY|1994|   5| 4.0|       2|
    |   A|  YY|1995|  10| 2.0|       3|
    |   A|  YY|1996|   2| 7.0|       4|
    +----+----+----+----+----+--------+
    * */
        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap())
                .containsExactly(
                        Map.of(
                                "Id_1", "A", "Id_2", "XX", "Year", 1993L, "Me_1", 3L, "Me_2", 1.0D, "rank_col", 1L),
                        Map.of(
                                "Id_1", "A", "Id_2", "XX", "Year", 1994L, "Me_1", 4L, "Me_2", 9.0D, "rank_col", 2L),
                        Map.of(
                                "Id_1", "A", "Id_2", "XX", "Year", 1995L, "Me_1", 7L, "Me_2", 5.0D, "rank_col", 3L),
                        Map.of(
                                "Id_1", "A", "Id_2", "XX", "Year", 1996L, "Me_1", 6L, "Me_2", 8.0D, "rank_col", 4L),
                        Map.of(
                                "Id_1", "A", "Id_2", "YY", "Year", 1993L, "Me_1", 9L, "Me_2", 3.0D, "rank_col", 1L),
                        Map.of(
                                "Id_1", "A", "Id_2", "YY", "Year", 1994L, "Me_1", 5L, "Me_2", 4.0D, "rank_col", 2L),
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
                                "rank_col",
                                3L),
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
                                "rank_col",
                                4L));
    }

    @Test
    public void testAnRankDesc() throws ScriptException {

        // Analytical function Test case 2 : rank on window with partition and desc order
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
                "res := ds2 [calc rank_col:= rank ( over ( partition by Id_1, Id_2 order by Year desc) )];");
        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);

    /*
    *
    +----+----+----+----+----+--------+
    |Id_1|Id_2|Year|Me_1|Me_2|rank_col|
    +----+----+----+----+----+--------+
    |   A|  XX|1996|   6| 8.0|       1|
    |   A|  XX|1995|   7| 5.0|       2|
    |   A|  XX|1994|   4| 9.0|       3|
    |   A|  XX|1993|   3| 1.0|       4|
    |   A|  YY|1996|   2| 7.0|       1|
    |   A|  YY|1995|  10| 2.0|       2|
    |   A|  YY|1994|   5| 4.0|       3|
    |   A|  YY|1993|   9| 3.0|       4|
    +----+----+----+----+----+--------+
    * */
        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap())
                .containsExactly(
                        Map.of(
                                "Id_1", "A", "Id_2", "XX", "Year", 1996L, "Me_1", 6L, "Me_2", 8.0D, "rank_col", 1L),
                        Map.of(
                                "Id_1", "A", "Id_2", "XX", "Year", 1995L, "Me_1", 7L, "Me_2", 5.0D, "rank_col", 2L),
                        Map.of(
                                "Id_1", "A", "Id_2", "XX", "Year", 1994L, "Me_1", 4L, "Me_2", 9.0D, "rank_col", 3L),
                        Map.of(
                                "Id_1", "A", "Id_2", "XX", "Year", 1993L, "Me_1", 3L, "Me_2", 1.0D, "rank_col", 4L),
                        Map.of(
                                "Id_1", "A", "Id_2", "YY", "Year", 1996L, "Me_1", 2L, "Me_2", 7.0D, "rank_col", 1L),
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
                                "rank_col",
                                2L),
                        Map.of(
                                "Id_1", "A", "Id_2", "YY", "Year", 1994L, "Me_1", 5L, "Me_2", 4.0D, "rank_col", 3L),
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
                                "rank_col",
                                4L));
    }
}
