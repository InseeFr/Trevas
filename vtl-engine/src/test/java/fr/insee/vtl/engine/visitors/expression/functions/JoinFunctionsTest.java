package fr.insee.vtl.engine.visitors.expression.functions;

import fr.insee.vtl.engine.exceptions.InvalidArgumentException;
import fr.insee.vtl.model.utils.Java8Helpers;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.InMemoryDataset;
import fr.insee.vtl.model.Structured;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import static fr.insee.vtl.engine.VtlScriptEngineTest.atPosition;
import static fr.insee.vtl.model.Dataset.Role;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class JoinFunctionsTest {

    private final InMemoryDataset ds1 = new InMemoryDataset(
            Java8Helpers.listOf(
                    Java8Helpers.listOf("a", 1L, 1L),
                    Java8Helpers.listOf("a", 2L, 2L),
                    Java8Helpers.listOf("b", 1L, 3L),
                    Java8Helpers.listOf("b", 2L, 4L),
                    Java8Helpers.listOf("c", 1L, 5L),
                    Java8Helpers.listOf("c", 2L, 6L)
            ),
            Java8Helpers.listOf(
                    new Structured.Component("id1", String.class, Role.IDENTIFIER),
                    new Structured.Component("id2", Long.class, Role.IDENTIFIER),
                    new Structured.Component("m1", Long.class, Role.MEASURE)
            )
    );
    private final InMemoryDataset ds2 = new InMemoryDataset(
            Java8Helpers.listOf(
                    Java8Helpers.listOf("a", 1L, 7L),
                    Java8Helpers.listOf("a", 2L, 8L),
                    Java8Helpers.listOf("b", 1L, 9L),
                    Java8Helpers.listOf("b", 2L, 10L),
                    Java8Helpers.listOf("d", 3L, 11L),
                    Java8Helpers.listOf("d", 4L, 12L)
            ),
            Java8Helpers.listOf(
                    new Structured.Component("id1", String.class, Role.IDENTIFIER),
                    new Structured.Component("id2", Long.class, Role.IDENTIFIER),
                    new Structured.Component("m2", Long.class, Role.MEASURE)
            )
    );
    private ScriptEngine engine;

    @BeforeEach
    public void setUp() {
        engine = new ScriptEngineManager().getEngineByName("vtl");
    }

    @Test
    public void testJoinWithExpressionFails() {
        engine.getContext().setAttribute("ds1", ds1, ScriptContext.ENGINE_SCOPE);
        engine.getContext().setAttribute("ds2", ds2, ScriptContext.ENGINE_SCOPE);

        assertThatThrownBy(() -> engine.eval("result := left_join(ds1[filter true], ds2[filter true]);"))
                .is(atPosition(0, 20, 36))
                .hasMessage("cannot use expression without alias in join clause");
        assertThatThrownBy(() -> engine.eval("result := inner_join(ds1, ds2[filter true]);"))
                .is(atPosition(0, 26, 42))
                .hasMessage("cannot use expression without alias in join clause");
        assertThatThrownBy(() -> engine.eval("result := cross_join(ds1[filter true], ds2);"))
                .is(atPosition(0, 21, 37))
                .hasMessage("cannot use expression without alias in join clause");
        assertThatThrownBy(() -> engine.eval("result := full_join(ds1[filter true], ds2[filter true]);"))
                .is(atPosition(0, 20, 36))
                .hasMessage("cannot use expression without alias in join clause");
    }

    @Test
    public void testLeftJoinWithUsing() throws ScriptException {

        engine.getContext().setAttribute("ds1", ds1, ScriptContext.ENGINE_SCOPE);
        engine.getContext().setAttribute("ds2", ds2, ScriptContext.ENGINE_SCOPE);
        engine.eval("result := left_join(ds1, ds2 as aliasDs using id1);");

        Dataset result = (Dataset) engine.getContext().getAttribute("result");
        assertThat(result.getColumnNames()).containsExactlyInAnyOrder(
                "id1", "id2", "m1", "m2"
        );
        assertThat(result.getDataAsList()).containsExactlyInAnyOrder(
                Arrays.asList("a", 1L, 1L, 7L),
                Arrays.asList("a", 1L, 2L, 8L),
                Arrays.asList("a", 2L, 1L, 7L),
                Arrays.asList("a", 2L, 2L, 8L),
                Arrays.asList("b", 3L, 1L, 9L),
                Arrays.asList("b", 3L, 2L, 10L),
                Arrays.asList("b", 4L, 1L, 9L),
                Arrays.asList("b", 4L, 2L, 10L),
                Arrays.asList("c", 5L, null, null),
                Arrays.asList("c", 6L, null, null)
        );
    }

    @Test
    public void testLeftJoinWithDifferentIdentifiers() throws ScriptException {
        InMemoryDataset dataset1 = new InMemoryDataset(
                Java8Helpers.listOf(
                        Java8Helpers.listOf(1L, 1L),
                        Java8Helpers.listOf(1L, 2L),
                        Java8Helpers.listOf(2L, 1L)
                ),
                Java8Helpers.listOf(
                        new Structured.Component("Id_1", Long.class, Role.IDENTIFIER),
                        new Structured.Component("Id_2", Long.class, Role.IDENTIFIER)
                )
        );
        InMemoryDataset dataset2 = new InMemoryDataset(
                Java8Helpers.listOf(
                        Java8Helpers.listOf(1L, "X"),
                        Java8Helpers.listOf(2L, "Y")
                ),
                Java8Helpers.listOf(
                        new Structured.Component("Id_2", Long.class, Role.IDENTIFIER),
                        new Structured.Component("Me_1", String.class, Role.MEASURE)
                )
        );

        ScriptContext context = engine.getContext();
        context.getBindings(ScriptContext.ENGINE_SCOPE).put("ds_1", dataset1);
        context.getBindings(ScriptContext.ENGINE_SCOPE).put("ds_2", dataset2);

        engine.eval("result := left_join(ds_1, ds_2 using Id_2);");
        Dataset result = ((Dataset) engine.getContext().getAttribute("result"));
        assertThat(result.getDataAsMap()).containsExactlyInAnyOrder(
                Java8Helpers.mapOf("Id_1", 1L, "Id_2", 1L, "Me_1", "X"),
                Java8Helpers.mapOf("Id_1", 1L, "Id_2", 2L, "Me_1", "Y"),
                Java8Helpers.mapOf("Id_1", 2L, "Id_2", 1L, "Me_1", "X")
        );
        assertThat(result.getDataStructure().get("Id_1").getRole()).isEqualTo(Role.IDENTIFIER);
        assertThat(result.getDataStructure().get("Id_2").getRole()).isEqualTo(Role.IDENTIFIER);
        assertThat(result.getDataStructure().get("Me_1").getRole()).isEqualTo(Role.MEASURE);

        assertThatThrownBy(() -> engine.eval("ds_1 := ds_1[calc measure Id_2 := Id_2];\n" +
                "result := left_join(ds_1, ds_2 using Id_2);"))
                .isInstanceOf(InvalidArgumentException.class)
                .hasMessage("using component Id_2 has to be an identifier");
    }

    @Test
    public void testLeftJoinWithDouble() throws ScriptException {
        InMemoryDataset dataset1 = new InMemoryDataset(
                Java8Helpers.listOf(
                        Java8Helpers.listOf("Hadrien", 10L),
                        Java8Helpers.listOf("Nico", 11L)
                ),
                Java8Helpers.listOf(
                        new Structured.Component("name", String.class, Role.IDENTIFIER),
                        new Structured.Component("age", Long.class, Role.MEASURE)
                )
        );
        InMemoryDataset dataset2 = new InMemoryDataset(
                Java8Helpers.listOf(
                        Java8Helpers.listOf("Hadrien", 20L),
                        Java8Helpers.listOf("Nico", 22L)
                ),
                Java8Helpers.listOf(
                        new Structured.Component("name", String.class, Role.IDENTIFIER),
                        new Structured.Component("age", Long.class, Role.MEASURE)
                )
        );

        ScriptContext context = engine.getContext();
        context.getBindings(ScriptContext.ENGINE_SCOPE).put("ds_1", dataset1);
        context.getBindings(ScriptContext.ENGINE_SCOPE).put("ds_2", dataset2);

        assertThatThrownBy(() -> engine.eval("result := left_join(ds_1, ds_2);"))
                .isInstanceOf(InvalidArgumentException.class)
                .hasMessage("It is not allowed that two or more Components in the virtual Data Set have the same name");

        engine.eval("result := left_join(ds_1 as ds1, ds_2 as ds2);");
        assertThat(((Dataset) engine.getContext().getAttribute("result")).getDataAsMap()).containsExactlyInAnyOrder(
                Java8Helpers.mapOf("name", "Hadrien", "age", 20L),
                Java8Helpers.mapOf("name", "Nico", "age", 22L)
        );

    }

    @Test
    public void testLeftJoinMixedStructure() throws ScriptException {

        InMemoryDataset dataset1 = new InMemoryDataset(
                Java8Helpers.listOf(
                        Java8Helpers.mapOf("id", "K001", "measure1", 1L, "measure2", 9L, "color", "yellow"),
                        Java8Helpers.mapOf("id", "K002", "measure1", 2L, "measure2", 8L, "color", "blue")
                ),
                Java8Helpers.mapOf("id", String.class, "measure1", Long.class, "measure2", Long.class, "color", String.class),
                Java8Helpers.mapOf("id", Role.IDENTIFIER, "measure1", Role.MEASURE, "measure2", Role.MEASURE, "color", Role.MEASURE)
        );

        InMemoryDataset dataset2 = new InMemoryDataset(
                Java8Helpers.listOf(
                        Java8Helpers.mapOf("id", "K001", "intermezzo", 0L, "measure2", 11L, "measure1", 19L, "stale", ""),
                        Java8Helpers.mapOf("id", "K003", "intermezzo", 0L, "measure2", 7L, "measure1", 3L, "stale", "")
                ),
                Java8Helpers.mapOf("id", String.class, "intermezzo", Long.class, "measure2", Long.class, "measure1", Long.class, "stale", String.class),
                Java8Helpers.mapOf("id", Role.IDENTIFIER, "intermezzo", Role.MEASURE, "measure2", Role.MEASURE, "measure1", Role.MEASURE, "stale", Role.MEASURE)
        );

        engine.getContext().setAttribute("ds1", dataset1, ScriptContext.ENGINE_SCOPE);
        engine.getContext().setAttribute("ds2", dataset2, ScriptContext.ENGINE_SCOPE);

        engine.eval("unionData := union(ds1[keep id, measure1, measure2], ds2[keep id, measure1, measure2]);");
        engine.eval("ds1_keep := ds1[keep id, color];");
        engine.eval("joinData := left_join(unionData, ds1_keep);");

        Dataset joinData = (Dataset) engine.getBindings(ScriptContext.ENGINE_SCOPE).get("joinData");

        // Build Map with null value
        Map<String, Object> thirdLine = new LinkedHashMap<>();
        thirdLine.put("id", "K003");
        thirdLine.put("measure1", 3L);
        thirdLine.put("measure2", 7L);
        thirdLine.put("color", null);

        assertThat(joinData.getDataAsMap()).containsExactlyInAnyOrder(
                Java8Helpers.mapOf("id", "K001", "measure1", 1L, "measure2", 9L, "color", "yellow"),
                Java8Helpers.mapOf("id", "K002", "measure1", 2L, "measure2", 8L, "color", "blue"),
                thirdLine
        );
    }

    @Test
    public void testInnerJoin() throws ScriptException {

        engine.getContext().setAttribute("ds_1", ds1, ScriptContext.ENGINE_SCOPE);
        engine.getContext().setAttribute("ds_2", ds2, ScriptContext.ENGINE_SCOPE);
        engine.eval("result := inner_join(ds_1[keep id1, id2, m1] as ds1, ds_2 as ds2);");

        Dataset result = (Dataset) engine.getContext().getAttribute("result");
        assertThat(result.getColumnNames()).containsExactlyInAnyOrder(
                "id1", "id2", "m1", "m2"
        );
        assertThat(result.getDataAsList()).containsExactlyInAnyOrder(
                Arrays.asList("a", 1L, 1L, 7L),
                Arrays.asList("a", 2L, 2L, 8L),
                Arrays.asList("b", 1L, 3L, 9L),
                Arrays.asList("b", 2L, 4L, 10L)
        );

        assertThatThrownBy(() -> engine.eval("ds_3 := ds_2[rename m2 to m1];" +
                "result := inner_join(ds_1, ds_3);"))
                .isInstanceOf(InvalidArgumentException.class)
                .hasMessage("It is not allowed that two or more Components in the virtual Data Set have the same name");

        engine.eval("result := inner_join(ds_1 as ds1, ds_2 as ds2 using id1);");

        result = (Dataset) engine.getContext().getAttribute("result");
        assertThat(result.getColumnNames()).containsExactlyInAnyOrder(
                "id1", "m1", "id2", "m2"
        );
        assertThat(result.getDataAsList()).containsExactlyInAnyOrder(
                Arrays.asList("a", 1L, 1L, 7L),
                Arrays.asList("a", 1L, 2L, 8L),
                Arrays.asList("a", 2L, 1L, 7L),
                Arrays.asList("a", 2L, 2L, 8L),
                Arrays.asList("b", 3L, 1L, 9L),
                Arrays.asList("b", 3L, 2L, 10L),
                Arrays.asList("b", 4L, 1L, 9L),
                Arrays.asList("b", 4L, 2L, 10L)
        );
    }

    @Test
    public void testFullJoin() throws ScriptException {
        ScriptContext context = engine.getContext();

        InMemoryDataset ds1 = new InMemoryDataset(
                Java8Helpers.listOf(
                        new Structured.Component("id", String.class, Role.IDENTIFIER),
                        new Structured.Component("m1", Long.class, Role.MEASURE)
                ),
                Arrays.asList("b", 1L),
                Arrays.asList("c", 2L),
                Arrays.asList("d", 3L)
        );

        InMemoryDataset ds2 = new InMemoryDataset(
                Java8Helpers.listOf(
                        new Structured.Component("id", String.class, Role.IDENTIFIER),
                        new Structured.Component("m1", Long.class, Role.MEASURE)
                ),
                Arrays.asList("a", 4L),
                Arrays.asList("b", 5L),
                Arrays.asList("c", 6L)
        );

        InMemoryDataset ds3 = new InMemoryDataset(
                Java8Helpers.listOf(
                        new Structured.Component("id", String.class, Role.IDENTIFIER),
                        new Structured.Component("m1", Long.class, Role.MEASURE)
                ),
                Arrays.asList("a", 7L),
                Arrays.asList("d", 8L)
        );

        context.getBindings(ScriptContext.ENGINE_SCOPE).put("ds_1", ds1);
        context.getBindings(ScriptContext.ENGINE_SCOPE).put("ds_2", ds2);
        context.getBindings(ScriptContext.ENGINE_SCOPE).put("ds_3", ds3);

        engine.eval("result := full_join(ds_1 as ds1, ds_2 as ds2, ds_3 as ds3);");

        Dataset result = (Dataset) context.getAttribute("result");

        assertThat(result.getDataStructure().values()).containsExactly(
                new Structured.Component("id", String.class, Role.IDENTIFIER),
                new Structured.Component("m1", Long.class, Role.MEASURE)
        );

        assertThat(result.getDataAsList()).containsExactlyInAnyOrder(
                Arrays.asList("d", 8L),
                Arrays.asList("c", null),
                Arrays.asList("b", null),
                Arrays.asList("a", 7L)
        );

    }

    @Test
    public void testCrossJoin() throws ScriptException {

        InMemoryDataset ds3 = new InMemoryDataset(
                Java8Helpers.listOf(
                        Java8Helpers.listOf("a", 1L, 1L),
                        Java8Helpers.listOf("a", 2L, 2L),
                        Java8Helpers.listOf("a", 3L, 3L),
                        Java8Helpers.listOf("b", 1L, 3L),
                        Java8Helpers.listOf("b", 2L, 4L)
                ),
                Java8Helpers.listOf(
                        new Structured.Component("id1", String.class, Role.IDENTIFIER),
                        new Structured.Component("id2", Long.class, Role.IDENTIFIER),
                        new Structured.Component("m1", Long.class, Role.MEASURE)
                )
        );

        InMemoryDataset ds4 = new InMemoryDataset(
                Java8Helpers.listOf(
                        Java8Helpers.listOf("a", 1L, 7L),
                        Java8Helpers.listOf("a", 2L, 8L)
                ),
                Java8Helpers.listOf(
                        new Structured.Component("id1", String.class, Role.IDENTIFIER),
                        new Structured.Component("id2", Long.class, Role.IDENTIFIER),
                        new Structured.Component("m2", Long.class, Role.MEASURE)
                )
        );

        engine.getContext().setAttribute("ds_3", ds3, ScriptContext.ENGINE_SCOPE);
        engine.getContext().setAttribute("ds_4", ds4, ScriptContext.ENGINE_SCOPE);
        engine.eval("result := cross_join(ds_3 as ds3, ds_4 as ds4);");

        Dataset result = (Dataset) engine.getContext().getAttribute("result");
        assertThat(result.getColumnNames()).containsExactly(
                "ds3#id1", "ds3#id2", "ds4#id1", "ds4#id2", "m1", "m2"
        );
        assertThat(result.getDataAsList()).containsExactlyInAnyOrder(
                Arrays.asList("a", 1L, "a", 1L, 1L, 7L),
                Arrays.asList("a", 1L, "a", 2L, 1L, 8L),
                Arrays.asList("a", 2L, "a", 1L, 2L, 7L),
                Arrays.asList("a", 2L, "a", 2L, 2L, 8L),
                Arrays.asList("a", 3L, "a", 1L, 3L, 7L),
                Arrays.asList("a", 3L, "a", 2L, 3L, 8L),
                Arrays.asList("b", 1L, "a", 1L, 3L, 7L),
                Arrays.asList("b", 1L, "a", 2L, 3L, 8L),
                Arrays.asList("b", 2L, "a", 1L, 4L, 7L),
                Arrays.asList("b", 2L, "a", 2L, 4L, 8L)
        );
    }
}
