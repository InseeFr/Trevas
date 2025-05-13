package fr.insee.vtl.spark.processing.engine;

import fr.insee.vtl.engine.VtlScriptEngine;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.InMemoryDataset;
import fr.insee.vtl.model.Structured;
import fr.insee.vtl.spark.SparkDataset;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class UnionTest {

    private final InMemoryDataset unionDS1 = new InMemoryDataset(
            List.of(
                    Map.of("Id_1", "2012", "Id_2", "B", "Id_3", "Total", "Id_4", "Total", "Me_1", 5L),
                    Map.of("Id_1", "2012", "Id_2", "G", "Id_3", "Total", "Id_4", "Total", "Me_1", 2L),
                    Map.of("Id_1", "2012", "Id_2", "F", "Id_3", "Total", "Id_4", "Total", "Me_1", 3L)

            ),
            Map.of("Id_1", String.class, "Id_2", String.class, "Id_3", String.class, "Id_4", String.class, "Me_1", Long.class),
            Map.of("Id_1", Dataset.Role.IDENTIFIER, "Id_2", Dataset.Role.IDENTIFIER, "Id_3", Dataset.Role.IDENTIFIER, "Id_4", Dataset.Role.IDENTIFIER, "Me_1", Dataset.Role.MEASURE)
    );
    private final InMemoryDataset unionDS2 = new InMemoryDataset(
            List.of(
                    Map.of("Id_1", "2012", "Id_2", "N", "Id_3", "Total", "Id_4", "Total", "Me_1", 23L),
                    Map.of("Id_1", "2012", "Id_2", "S", "Id_3", "Total", "Id_4", "Total", "Me_1", 5L)

            ),
            Map.of("Id_1", String.class, "Id_2", String.class, "Id_3", String.class, "Id_4", String.class, "Me_1", Long.class),
            Map.of("Id_1", Dataset.Role.IDENTIFIER, "Id_2", Dataset.Role.IDENTIFIER, "Id_3", Dataset.Role.IDENTIFIER, "Id_4", Dataset.Role.IDENTIFIER, "Me_1", Dataset.Role.MEASURE)
    );
    private final InMemoryDataset unionDS3 = new InMemoryDataset(
            List.of(
                    Map.of("Id_1", "2012", "Id_2", "L", "Id_3", "Total", "Id_4", "Total", "Me_1", 5L),
                    Map.of("Id_1", "2012", "Id_2", "M", "Id_3", "Total", "Id_4", "Total", "Me_1", 2L),
                    Map.of("Id_1", "2012", "Id_2", "X", "Id_3", "Total", "Id_4", "Total", "Me_1", 3L)
            ),
            Map.of("Id_1", String.class, "Id_2", String.class, "Id_3", String.class, "Id_4", String.class, "Me_1", Long.class),
            Map.of("Id_1", Dataset.Role.IDENTIFIER, "Id_2", Dataset.Role.IDENTIFIER, "Id_3", Dataset.Role.IDENTIFIER, "Id_4", Dataset.Role.IDENTIFIER, "Me_1", Dataset.Role.MEASURE)
    );
    private final InMemoryDataset unionDS4 = new InMemoryDataset(
            List.of(
                    Map.of("Id_1", "2012", "Id_2", "N", "Id_3", "Total", "Id_4", "Total", "Me_1", 5L),
                    Map.of("Id_1", "2012", "Id_2", "S", "Id_3", "Total", "Id_4", "Total", "Me_1", 2L),
                    Map.of("Id_1", "2012", "Id_2", "X", "Id_3", "Total", "Id_4", "Total", "Me_1", 3L)
            ),
            Map.of("Id_1", String.class, "Id_2", String.class, "Id_3", String.class, "Id_4", String.class, "Me_1", Long.class),
            Map.of("Id_1", Dataset.Role.IDENTIFIER, "Id_2", Dataset.Role.IDENTIFIER, "Id_3", Dataset.Role.IDENTIFIER, "Id_4", Dataset.Role.IDENTIFIER, "Me_1", Dataset.Role.MEASURE)
    );
    private final InMemoryDataset unionDS5 = new InMemoryDataset(
            List.of(
                    Map.of("Id_1", "2012", "Id_2", "N", "Id_3", "Total", "Me_1", 5L),
                    Map.of("Id_1", "2012", "Id_2", "S", "Id_3", "Total", "Me_1", 2L),
                    Map.of("Id_1", "2012", "Id_2", "X", "Id_3", "Total", "Me_1", 3L)
            ),
            Map.of("Id_1", String.class, "Id_2", String.class, "Id_3", String.class, "Me_1", Long.class),
            Map.of("Id_1", Dataset.Role.IDENTIFIER, "Id_2", Dataset.Role.IDENTIFIER, "Id_3", Dataset.Role.IDENTIFIER, "Me_1", Dataset.Role.MEASURE)
    );
    private final String DEFAULT_NULL_STR = "null";
    private SparkSession spark;
    private ScriptEngine engine;

    private static <T, K> Map<K, T> replaceNullValues(Map<K, T> map, T defaultValue) {

        // Replace the null value
        map = map.entrySet()
                .stream()
                .map(entry -> {
                    if (entry.getValue() == null)
                        entry.setValue(defaultValue);
                    return entry;
                })
                .collect(Collectors.toMap(Map.Entry::getKey,
                        Map.Entry::getValue));

        return map;
    }

    @BeforeEach
    public void setUp() {

        ScriptEngineManager mgr = new ScriptEngineManager();
        engine = mgr.getEngineByExtension("vtl");

        spark = SparkSession.builder()
                .appName("test")
                .master("local")
                .getOrCreate();
        SparkSession.setActiveSession(spark);

        engine.put(VtlScriptEngine.PROCESSING_ENGINE_NAMES, "spark");
    }

    @AfterEach
    public void tearDown() {
        if (spark != null)
            spark.close();
    }

    @Test
    public void testDataStructureAfterUnion() throws ScriptException {
        // Union Test case 1 : After union, the result should have the same data structure as the input dataframe
        /* Input dataset ds1 and ds2 has below data structure
       Component{Id_1, type=class java.lang.String, role=IDENTIFIER}
       Component{Id_2, type=class java.lang.String, role=IDENTIFIER}
       Component{Id_3, type=class java.lang.String, role=IDENTIFIER}
       Component{Id_4, type=class java.lang.String, role=IDENTIFIER}
       Component{Me_1, type=class java.lang.Long, role=MEASURE}
        * */
        ScriptContext context = engine.getContext();
        context.setAttribute("ds1", unionDS1, ScriptContext.ENGINE_SCOPE);
        context.setAttribute("ds2", unionDS2, ScriptContext.ENGINE_SCOPE);
        engine.eval("res := union (ds1, ds2);");
        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);

        /*
         * the result df should have the same data structure
         * */

        Collection<Structured.Component> actualStructure = ((SparkDataset) engine.getContext().getAttribute("res")).getDataStructure().values();

        assertThat(actualStructure).containsExactlyInAnyOrder(
                new Structured.Component("Id_1", String.class, Dataset.Role.IDENTIFIER),
                new Structured.Component("Id_2", String.class, Dataset.Role.IDENTIFIER),
                new Structured.Component("Id_3", String.class, Dataset.Role.IDENTIFIER),
                new Structured.Component("Id_4", String.class, Dataset.Role.IDENTIFIER),
                new Structured.Component("Me_1", Long.class, Dataset.Role.MEASURE)
        );
    }

    @Test
    public void testUnionTwoWithoutDup() throws ScriptException {
        // Union Test case 1 : union on two df without duplicate
        /* Input dataset ds1
        +----+----+-----+-----+----+
        |Id_1|Id_2| Id_3| Id_4|Me_1|
        +----+----+-----+-----+----+
        |2012|   B|Total|Total|   5|
        |2012|   G|Total|Total|   2|
        |2012|   F|Total|Total|   3|
        +----+----+-----+-----+----+

        ****************************
        * Input dataset ds2
        +----+----+-----+-----+----+
        |Id_1|Id_2| Id_3| Id_4|Me_1|
        +----+----+-----+-----+----+
        |2012|   N|Total|Total|  23|
        |2012|   S|Total|Total|   5|
        +----+----+-----+-----+----+
        * */
        ScriptContext context = engine.getContext();
        context.setAttribute("ds1", unionDS1, ScriptContext.ENGINE_SCOPE);
        context.setAttribute("ds2", unionDS2, ScriptContext.ENGINE_SCOPE);
        engine.eval("res := union (ds1, ds2);");
        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);

        /*
        * result df
        +----+----+-----+-----+----+
        |Id_1|Id_2| Id_3| Id_4|Me_1|
        +----+----+-----+-----+----+
        |2012|   B|Total|Total|   5|
        |2012|   G|Total|Total|   2|
        |2012|   F|Total|Total|   3|
        |2012|   N|Total|Total|  23|
        |2012|   S|Total|Total|   5|
        +----+----+-----+-----+----+
        * */

        List<Map<String, Object>> actualWithNull = ((Dataset) engine.getContext().getAttribute("res")).getDataAsMap();

        List<Map<String, Object>> actual = new ArrayList<>();
        for (Map<String, Object> map : actualWithNull) {
            actual.add(replaceNullValues(map, DEFAULT_NULL_STR));
        }
        List<Map<String, Object>> expected = List.of(Map.of("Id_1", "2012", "Id_2", "B", "Id_3", "Total", "Id_4", "Total", "Me_1", 5L),
                Map.of("Id_1", "2012", "Id_2", "G", "Id_3", "Total", "Id_4", "Total", "Me_1", 2L),
                Map.of("Id_1", "2012", "Id_2", "F", "Id_3", "Total", "Id_4", "Total", "Me_1", 3L),
                Map.of("Id_1", "2012", "Id_2", "N", "Id_3", "Total", "Id_4", "Total", "Me_1", 23L),
                Map.of("Id_1", "2012", "Id_2", "S", "Id_3", "Total", "Id_4", "Total", "Me_1", 5L)
        );
        assertEquals(new HashSet<>(actual), new HashSet<>(expected));
    }

    @Test
    public void testUnionThreeWithoutDup() throws ScriptException {
        // Union Test case 2 : union on three df without duplicate
        /* Input dataset ds1
        +----+----+-----+-----+----+
        |Id_1|Id_2| Id_3| Id_4|Me_1|
        +----+----+-----+-----+----+
        |2012|   B|Total|Total|   5|
        |2012|   G|Total|Total|   2|
        |2012|   F|Total|Total|   3|
        +----+----+-----+-----+----+

        ****************************
        * Input dataset ds2
        +----+----+-----+-----+----+
        |Id_1|Id_2| Id_3| Id_4|Me_1|
        +----+----+-----+-----+----+
        |2012|   N|Total|Total|  23|
        |2012|   S|Total|Total|   5|
        +----+----+-----+-----+----+

        **************************
        * Input dataset ds3
        +----+----+-----+-----+----+
        |Id_1|Id_2| Id_3| Id_4|Me_1|
        +----+----+-----+-----+----+
        |2012|   L|Total|Total|   5|
        |2012|   M|Total|Total|   2|
        |2012|   X|Total|Total|   3|
        +----+----+-----+-----+----+
        * */
        ScriptContext context = engine.getContext();
        context.setAttribute("ds1", unionDS1, ScriptContext.ENGINE_SCOPE);
        context.setAttribute("ds2", unionDS2, ScriptContext.ENGINE_SCOPE);
        context.setAttribute("ds3", unionDS3, ScriptContext.ENGINE_SCOPE);
        engine.eval("res := union (ds1, ds2, ds3);");
        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);

        /*
        * result df
        +----+----+-----+-----+----+
        |Id_1|Id_2| Id_3| Id_4|Me_1|
        +----+----+-----+-----+----+
        |2012|   B|Total|Total|   5|
        |2012|   G|Total|Total|   2|
        |2012|   F|Total|Total|   3|
        |2012|   N|Total|Total|  23|
        |2012|   S|Total|Total|   5|
        +----+----+-----+-----+----+
        * */

        List<Map<String, Object>> actualWithNull = ((Dataset) engine.getContext().getAttribute("res")).getDataAsMap();

        List<Map<String, Object>> actual = new ArrayList<>();
        for (Map<String, Object> map : actualWithNull) {
            actual.add(replaceNullValues(map, DEFAULT_NULL_STR));
        }
        List<Map<String, Object>> expected = List.of(
                Map.of("Id_1", "2012", "Id_2", "B", "Id_3", "Total", "Id_4", "Total", "Me_1", 5L),
                Map.of("Id_1", "2012", "Id_2", "G", "Id_3", "Total", "Id_4", "Total", "Me_1", 2L),
                Map.of("Id_1", "2012", "Id_2", "F", "Id_3", "Total", "Id_4", "Total", "Me_1", 3L),
                Map.of("Id_1", "2012", "Id_2", "S", "Id_3", "Total", "Id_4", "Total", "Me_1", 5L),
                Map.of("Id_1", "2012", "Id_2", "N", "Id_3", "Total", "Id_4", "Total", "Me_1", 23L),
                Map.of("Id_1", "2012", "Id_2", "X", "Id_3", "Total", "Id_4", "Total", "Me_1", 3L),
                Map.of("Id_1", "2012", "Id_2", "M", "Id_3", "Total", "Id_4", "Total", "Me_1", 2L),
                Map.of("Id_1", "2012", "Id_2", "L", "Id_3", "Total", "Id_4", "Total", "Me_1", 5L)
        );
        assertEquals(new HashSet<>(actual), new HashSet<>(expected));
    }

    @Test
    public void testUnionThreeWithDup() throws ScriptException {
        // Union Test case 3 : union on three df with duplicate, note for the duplicated rows, the first appeared row
        // are kept, the rest all dropped
        /* Input dataset ds1
        +----+----+-----+-----+----+
        |Id_1|Id_2| Id_3| Id_4|Me_1|
        +----+----+-----+-----+----+
        |2012|   B|Total|Total|   5|
        |2012|   G|Total|Total|   2|
        |2012|   F|Total|Total|   3|
        +----+----+-----+-----+----+

        ****************************
        * Input dataset ds2
        +----+----+-----+-----+----+
        |Id_1|Id_2| Id_3| Id_4|Me_1|
        +----+----+-----+-----+----+
        |2012|   N|Total|Total|  23|
        |2012|   S|Total|Total|   5|
        +----+----+-----+-----+----+

        **************************
        * Input dataset ds4
        +----+----+-----+-----+----+
        |Id_1|Id_2| Id_3| Id_4|Me_1|
        +----+----+-----+-----+----+
        |2012|   N|Total|Total|   5|
        |2012|   S|Total|Total|   2|
        |2012|   X|Total|Total|   3|
        +----+----+-----+-----+----+
        * */
        ScriptContext context = engine.getContext();
        context.setAttribute("ds1", unionDS1, ScriptContext.ENGINE_SCOPE);
        context.setAttribute("ds2", unionDS2, ScriptContext.ENGINE_SCOPE);
        context.setAttribute("ds4", unionDS4, ScriptContext.ENGINE_SCOPE);
        engine.eval("res := union (ds1, ds2, ds4);");
        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);

        /*
        * result df
        +----+----+-----+-----+----+
        |Id_1|Id_2| Id_3| Id_4|Me_1|
        +----+----+-----+-----+----+
        |2012|   F|Total|Total|   3|
        |2012|   B|Total|Total|   5|
        |2012|   G|Total|Total|   2|
        |2012|   N|Total|Total|  23|
        |2012|   S|Total|Total|   5|
        |2012|   X|Total|Total|   3|
        +----+----+-----+-----+----+
        * */

        List<Map<String, Object>> actualWithNull = ((Dataset) engine.getContext().getAttribute("res")).getDataAsMap();

        List<Map<String, Object>> actual = new ArrayList<>();
        for (Map<String, Object> map : actualWithNull) {
            actual.add(replaceNullValues(map, DEFAULT_NULL_STR));
        }
        List<Map<String, Object>> expected = List.of(
                Map.of("Id_1", "2012", "Id_2", "G", "Id_3", "Total", "Id_4", "Total", "Me_1", 2L),
                Map.of("Id_1", "2012", "Id_2", "F", "Id_3", "Total", "Id_4", "Total", "Me_1", 3L),
                Map.of("Id_1", "2012", "Id_2", "B", "Id_3", "Total", "Id_4", "Total", "Me_1", 5L),
                Map.of("Id_1", "2012", "Id_2", "S", "Id_3", "Total", "Id_4", "Total", "Me_1", 5L),
                Map.of("Id_1", "2012", "Id_2", "N", "Id_3", "Total", "Id_4", "Total", "Me_1", 23L),
                Map.of("Id_1", "2012", "Id_2", "X", "Id_3", "Total", "Id_4", "Total", "Me_1", 3L)
        );
        assertEquals(new HashSet<>(actual), new HashSet<>(expected));
    }

    @Test
    public void testUnionIncompatibleSchema() throws ScriptException {
        // Union Test case 4 : union on three df with incompatible schema, should throw exception
        // here we assert if the right exception is thrown.
        /* Input dataset ds1
        +----+----+-----+-----+----+
        |Id_1|Id_2| Id_3| Id_4|Me_1|
        +----+----+-----+-----+----+
        |2012|   B|Total|Total|   5|
        |2012|   G|Total|Total|   2|
        |2012|   F|Total|Total|   3|
        +----+----+-----+-----+----+

        ****************************
        * Input dataset ds2
        +----+----+-----+-----+----+
        |Id_1|Id_2| Id_3| Id_4|Me_1|
        +----+----+-----+-----+----+
        |2012|   N|Total|Total|  23|
        |2012|   S|Total|Total|   5|
        +----+----+-----+-----+----+

        **************************
        * Input dataset ds5
        +----+----+-----+----+
        |Id_1|Id_2| Id_3|Me_1|
        +----+----+-----+----+
        |2012|   N|Total|   5|
        |2012|   S|Total|   2|
        |2012|   X|Total|   3|
        +----+----+-----+----+
        * */
        ScriptContext context = engine.getContext();
        context.setAttribute("ds1", unionDS1, ScriptContext.ENGINE_SCOPE);
        context.setAttribute("ds2", unionDS2, ScriptContext.ENGINE_SCOPE);
        context.setAttribute("ds5", unionDS5, ScriptContext.ENGINE_SCOPE);

        // engine.getContext().getAttribute("res");
        Exception exception = assertThrows(fr.insee.vtl.engine.exceptions.InvalidArgumentException.class, () -> {
            engine.eval("res := union ( ds1, ds2, ds5 ) ;");
        });
        String expectedMessage = "dataset structure of ds5 is incompatible";
        String actualMessage = exception.getMessage();
        System.out.println(actualMessage);
        assertTrue(actualMessage.contains(expectedMessage));
    }
}
