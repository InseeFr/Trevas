package fr.insee.vtl.spark.processing.engine.analytic;

import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.InMemoryDataset;
import org.junit.jupiter.api.Test;

import javax.script.ScriptContext;
import javax.script.ScriptException;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class AnalyticRatioToReportTest extends AnalyticTest {

    @Test
    public void testAnRatioToReportWithCalcClause() throws ScriptException {

        InMemoryDataset anDS = new InMemoryDataset(
                List.of(
                        Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 3L, "Me_2", 1D),
                        Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 4L, "Me_2", 3D),
                        Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 7L, "Me_2", 5D),
                        Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 6L, "Me_2", 1D),
                        Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 12L, "Me_2", 0D),
                        Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 8L, "Me_2", 8D),
                        Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 6L, "Me_2", 5D),
                        Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 14L, "Me_2", -3D)

                ),
                Map.of("Id_1", String.class, "Id_2", String.class, "Year", Long.class, "Me_1", Long.class, "Me_2", Double.class),
                Map.of("Id_1", Dataset.Role.IDENTIFIER, "Id_2", Dataset.Role.IDENTIFIER, "Year", Dataset.Role.IDENTIFIER, "Me_1", Dataset.Role.MEASURE, "Me_2", Dataset.Role.MEASURE)
        );
        /* Input dataset
        +----+----+----+----+----+
        |Id_1|Id_2|Id_3|Me_1|Me_2|
        +----+----+----+----+----+
        |   A|  XX|2000|   3|   1|
        |   A|  XX|2001|   4|   3|
        |   A|  XX|2002|   7|   5|
        |   A|  XX|2003|   6|   1|
        |   A|  YY|2000|  12|   0|
        |   A|  YY|2001|   8|   8|
        |   A|  YY|2002|   6|   5|
        |   A|  YY|2003|  14|  -3|
        +----+----+----+----+----+
        * */
        ScriptContext context = engine.getContext();
        context.setAttribute("ds", anDS, ScriptContext.ENGINE_SCOPE);

        engine.eval("res :=  ds  [ calc ratio_Me_1 := ratio_to_report ( Me_1 over ( partition by Id_1, Id_2 ) )];");
        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);

        /*
        *
        +----+----+----+----+----+----------+
        |Id_1|Id_2|Id_3|Me_1|Me_2|ratio_Me_1|
        +----+----+----+----+----+----------+
        |   A|  XX|2000|   3|   1|      0.15|
        |   A|  XX|2001|   4|   3|       0.2|
        |   A|  XX|2002|   7|   5|      0.35|
        |   A|  XX|2003|   6|   1|       0.3|
        |   A|  YY|2000|  12|   0|       0.3|
        |   A|  YY|2001|   8|   8|       0.2|
        |   A|  YY|2002|   6|   5|      0.15|
        |   A|  YY|2003|  14|  -3|      0.35|
        +----+----+----+----+----+----------+
        * */
        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 3L, "Me_2", 1.0D, "ratio_Me_1", 0.15D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 4L, "Me_2", 3.0D, "ratio_Me_1", 0.2D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 7L, "Me_2", 5.0D, "ratio_Me_1", 0.35D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 6L, "Me_2", 1.0D, "ratio_Me_1", 0.3D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 12L, "Me_2", 0.0D, "ratio_Me_1", 0.3D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 8L, "Me_2", 8.0D, "ratio_Me_1", 0.2D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 6L, "Me_2", 5.0D, "ratio_Me_1", 0.15D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 14L, "Me_2", -3.0D, "ratio_Me_1", 0.35D)
        );

    }

    /*
     * Test case for analytic function ratio_to_report
     * Analytic clause restriction:
     * - The orderClause and windowClause of the Analytic invocation syntax are not allowed.
     * */

    @Test
    public void testAnRatioToReport() throws ScriptException {

        InMemoryDataset anDS = new InMemoryDataset(
                List.of(
                        Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 3L, "Me_2", 1D),
                        Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 4L, "Me_2", 3D),
                        Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 7L, "Me_2", 5D),
                        Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 6L, "Me_2", 1D),
                        Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 12L, "Me_2", 0D),
                        Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 8L, "Me_2", 8D),
                        Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 6L, "Me_2", 5D),
                        Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 14L, "Me_2", -3D)

                ),
                Map.of("Id_1", String.class, "Id_2", String.class, "Year", Long.class, "Me_1", Long.class, "Me_2", Double.class),
                Map.of("Id_1", Dataset.Role.IDENTIFIER, "Id_2", Dataset.Role.IDENTIFIER, "Year", Dataset.Role.IDENTIFIER, "Me_1", Dataset.Role.MEASURE, "Me_2", Dataset.Role.MEASURE)
        );
        /* Input dataset
        +----+----+----+----+----+
        |Id_1|Id_2|Id_3|Me_1|Me_2|
        +----+----+----+----+----+
        |   A|  XX|2000|   3|   1|
        |   A|  XX|2001|   4|   3|
        |   A|  XX|2002|   7|   5|
        |   A|  XX|2003|   6|   1|
        |   A|  YY|2000|  12|   0|
        |   A|  YY|2001|   8|   8|
        |   A|  YY|2002|   6|   5|
        |   A|  YY|2003|  14|  -3|
        +----+----+----+----+----+
        * */
        ScriptContext context = engine.getContext();
        context.setAttribute("ds", anDS, ScriptContext.ENGINE_SCOPE);


        engine.eval("res := ratio_to_report ( ds over ( partition by Id_1, Id_2 ) );");
        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);

        /*
        *
        +----+----+----+----+----+----------+----------+----------+----------+
        |Id_1|Id_2|Id_3|Me_1|Me_2|total_Me_1|ratio_Me_1|total_Me_2|ratio_Me_2|
        +----+----+----+----+----+----------+----------+----------+----------+
        |   A|  XX|2000|   3|   1|        20|      0.15|        10|       0.1|
        |   A|  XX|2001|   4|   3|        20|       0.2|        10|       0.3|
        |   A|  XX|2002|   7|   5|        20|      0.35|        10|       0.5|
        |   A|  XX|2003|   6|   1|        20|       0.3|        10|       0.1|
        |   A|  YY|2000|  12|   0|        40|       0.3|        10|       0.0|
        |   A|  YY|2001|   8|   8|        40|       0.2|        10|       0.8|
        |   A|  YY|2002|   6|   5|        40|      0.15|        10|       0.5|
        |   A|  YY|2003|  14|  -3|        40|      0.35|        10|      -0.3|
        +----+----+----+----+----+----------+----------+----------+----------+
        * */
        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2000L, "Me_1", 0.15D, "Me_2", 0.1D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2001L, "Me_1", 0.2D, "Me_2", 0.3D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2002L, "Me_1", 0.35D, "Me_2", 0.5D),
                Map.of("Id_1", "A", "Id_2", "XX", "Year", 2003L, "Me_1", 0.3D, "Me_2", 0.1D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2000L, "Me_1", 0.3D, "Me_2", 0.0D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2001L, "Me_1", 0.2D, "Me_2", 0.8D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2002L, "Me_1", 0.15D, "Me_2", 0.5D),
                Map.of("Id_1", "A", "Id_2", "YY", "Year", 2003L, "Me_1", 0.35D, "Me_2", -0.3D)
        );


    }

    }