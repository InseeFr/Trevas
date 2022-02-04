package fr.insee.vtl.engine.visitors.expression.functions;

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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static fr.insee.vtl.engine.VtlScriptEngineTest.atPosition;
import static fr.insee.vtl.model.Dataset.Role;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class JoinFunctionsTest {

    private ScriptEngine engine;

    private InMemoryDataset ds1 = new InMemoryDataset(
            List.of(
                    List.of("a", 1L, 1L),
                    List.of("a", 2L, 2L),
                    List.of("b", 1L, 3L),
                    List.of("b", 2L, 4L),
                    List.of("c", 1L, 5L),
                    List.of("c", 2L, 6L)
            ),
            List.of(
                    new Structured.Component("id1", String.class, Role.IDENTIFIER),
                    new Structured.Component("id2", Long.class, Role.IDENTIFIER),
                    new Structured.Component("m1", Long.class, Role.MEASURE)
            )
    );

    private InMemoryDataset ds2 = new InMemoryDataset(
            List.of(
                    List.of("a", 1L, 7L),
                    List.of("a", 2L, 8L),
                    List.of("b", 1L, 9L),
                    List.of("b", 2L, 10L),
                    List.of("d", 3L, 11L),
                    List.of("d", 4L, 12L)
            ),
            List.of(
                    new Structured.Component("id1", String.class, Role.IDENTIFIER),
                    new Structured.Component("id2", Long.class, Role.IDENTIFIER),
                    new Structured.Component("m2", Long.class, Role.MEASURE)
            )
    );

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
                .hasMessage("cannot use expression in join clause");
        assertThatThrownBy(() -> engine.eval("result := inner_join(ds1, ds2[filter true]);"))
                .is(atPosition(0, 26, 42))
                .hasMessage("cannot use expression in join clause");
        assertThatThrownBy(() -> engine.eval("result := cross_join(ds1[filter true], ds2);"))
                .is(atPosition(0, 21, 37))
                .hasMessage("cannot use expression in join clause");
        assertThatThrownBy(() -> engine.eval("result := full_join(ds1[filter true], ds2[filter true]);"))
                .is(atPosition(0, 20, 36))
                .hasMessage("cannot use expression in join clause");
    }

    @Test
    public void testLeftJoinWithUsing() throws ScriptException {

        engine.getContext().setAttribute("ds1", ds1, ScriptContext.ENGINE_SCOPE);
        engine.getContext().setAttribute("ds2", ds2, ScriptContext.ENGINE_SCOPE);
        engine.eval("result := left_join(ds1, ds2 as aliasDs using id1);");

        var result = (Dataset) engine.getContext().getAttribute("result");
        assertThat(result.getColumnNames()).containsExactlyInAnyOrder(
                "id1", "ds1#id2", "m1", "aliasDs#id2", "m2"
        );
        assertThat(result.getDataAsList()).containsExactlyInAnyOrder(
                Arrays.asList("a", 1L, 1L, 1L, 7L),
                Arrays.asList("a", 1L, 1L, 2L, 8L),
                Arrays.asList("a", 2L, 2L, 1L, 7L),
                Arrays.asList("a", 2L, 2L, 2L, 8L),
                Arrays.asList("b", 1L, 3L, 1L, 9L),
                Arrays.asList("b", 1L, 3L, 2L, 10L),
                Arrays.asList("b", 2L, 4L, 1L, 9L),
                Arrays.asList("b", 2L, 4L, 2L, 10L),
                Arrays.asList("c", 1L, 5L, null, null),
                Arrays.asList("c", 2L, 6L, null, null)
        );
    }

    @Test
    public void testLeftJoinWithDouble() throws ScriptException {
        InMemoryDataset dataset1 = new InMemoryDataset(
                List.of(
                        List.of("Hadrien", 10L),
                        List.of("Nico", 11L)
                ),
                List.of(
                        new Structured.Component("name", String.class, Role.IDENTIFIER),
                        new Structured.Component("age", Long.class, Role.MEASURE)
                )
        );
        InMemoryDataset dataset2 = new InMemoryDataset(
                List.of(
                        List.of("Hadrien", 20L),
                        List.of("Nico", 22L)
                ),
                List.of(
                        new Structured.Component("name", String.class, Role.IDENTIFIER),
                        new Structured.Component("age", Long.class, Role.MEASURE)
                )
        );

        ScriptContext context = engine.getContext();
        context.getBindings(ScriptContext.ENGINE_SCOPE).put("ds1", dataset1);
        context.getBindings(ScriptContext.ENGINE_SCOPE).put("ds2", dataset2);

        engine.eval("result := left_join(ds1, ds2);");
        assertThat(((Dataset) engine.getContext().getAttribute("result")).getDataAsMap()).containsExactlyInAnyOrder(
                Map.of("name", "Hadrien", "ds1#age", 10L, "ds2#age", 20L),
                Map.of("name", "Nico", "ds1#age", 11L, "ds2#age", 22L)
        );

    }
    
    @Test
    public void testInnerJoin() throws ScriptException {

        engine.getContext().setAttribute("ds1", ds1, ScriptContext.ENGINE_SCOPE);
        engine.getContext().setAttribute("ds2", ds2, ScriptContext.ENGINE_SCOPE);
        engine.eval("result := inner_join(ds1, ds2);");

        var result = (Dataset) engine.getContext().getAttribute("result");
        assertThat(result.getColumnNames()).containsExactlyInAnyOrder(
                "id1", "id2", "m1", "m2"
        );
        assertThat(result.getDataAsList()).containsExactlyInAnyOrder(
                Arrays.asList("a", 1L, 1L, 7L),
                Arrays.asList("a", 2L, 2L, 8L),
                Arrays.asList("b", 1L, 3L, 9L),
                Arrays.asList("b", 2L, 4L, 10L)
        );

        engine.eval("result := inner_join(ds1, ds2 using id1);");

        result = (Dataset) engine.getContext().getAttribute("result");
        assertThat(result.getColumnNames()).containsExactlyInAnyOrder(
                "id1", "ds1#id2", "m1", "ds2#id2", "m2"
        );
        assertThat(result.getDataAsList()).containsExactlyInAnyOrder(
                Arrays.asList("a", 1L, 1L, 1L, 7L),
                Arrays.asList("a", 1L, 1L, 2L, 8L),
                Arrays.asList("a", 2L, 2L, 1L, 7L),
                Arrays.asList("a", 2L, 2L, 2L, 8L),
                Arrays.asList("b", 1L, 3L, 1L, 9L),
                Arrays.asList("b", 1L, 3L, 2L, 10L),
                Arrays.asList("b", 2L, 4L, 1L, 9L),
                Arrays.asList("b", 2L, 4L, 2L, 10L)
        );
    }

    @Test
    public void testFullJoin() throws ScriptException {
        ScriptContext context = engine.getContext();

        var ds1 = new InMemoryDataset(
                List.of(
                        new Structured.Component("id", String.class, Role.IDENTIFIER),
                        new Structured.Component("m1", Long.class, Role.MEASURE)
                ),
                Arrays.asList("b", 1L),
                Arrays.asList("c", 2L),
                Arrays.asList("d", 3L)
        );

        var ds2 = new InMemoryDataset(
                List.of(
                        new Structured.Component("id", String.class, Role.IDENTIFIER),
                        new Structured.Component("m1", Long.class, Role.MEASURE)
                ),
                Arrays.asList("a", 4L),
                Arrays.asList("b", 5L),
                Arrays.asList("c", 6L)
        );

        var ds3 = new InMemoryDataset(
                List.of(
                        new Structured.Component("id", String.class, Role.IDENTIFIER),
                        new Structured.Component("m1", Long.class, Role.MEASURE)
                ),
                Arrays.asList("a", 7L),
                Arrays.asList("d", 8L)
        );

        context.getBindings(ScriptContext.ENGINE_SCOPE).put("ds1", ds1);
        context.getBindings(ScriptContext.ENGINE_SCOPE).put("ds2", ds2);
        context.getBindings(ScriptContext.ENGINE_SCOPE).put("ds3", ds3);

        engine.eval("result := full_join(ds1 as dsOne, ds2, ds3);");

        var result = (Dataset) context.getAttribute("result");

        assertThat(result.getDataStructure().values()).containsExactly(
                new Structured.Component("id", String.class, Role.IDENTIFIER),
                new Structured.Component("dsOne#m1", Long.class, Role.MEASURE),
                new Structured.Component("ds2#m1", Long.class, Role.MEASURE),
                new Structured.Component("ds3#m1", Long.class, Role.MEASURE)
        );

        assertThat(result.getDataAsList()).containsExactlyInAnyOrder(
                Arrays.asList("d", 3L, null, 8L),
                Arrays.asList("c", 2L, 6L, null),
                Arrays.asList("b", 1L, 5L, null),
                Arrays.asList("a", null, 4L, 7L)
        );

    }

    @Test
    public void testCrossJoin() throws ScriptException {

        InMemoryDataset ds3 = new InMemoryDataset(
                List.of(
                        List.of("a", 1L, 1L),
                        List.of("a", 2L, 2L),
                        List.of("a", 3L, 3L),
                        List.of("b", 1L, 3L),
                        List.of("b", 2L, 4L)
                ),
                List.of(
                        new Structured.Component("id1", String.class, Role.IDENTIFIER),
                        new Structured.Component("id2", Long.class, Role.IDENTIFIER),
                        new Structured.Component("m1", Long.class, Role.MEASURE)
                )
        );

        InMemoryDataset ds4 = new InMemoryDataset(
                List.of(
                        List.of("a", 1L, 7L),
                        List.of("a", 2L, 8L)
                ),
                List.of(
                        new Structured.Component("id1", String.class, Role.IDENTIFIER),
                        new Structured.Component("id2", Long.class, Role.IDENTIFIER),
                        new Structured.Component("m2", Long.class, Role.MEASURE)
                )
        );

        engine.getContext().setAttribute("ds3", ds3, ScriptContext.ENGINE_SCOPE);
        engine.getContext().setAttribute("ds4", ds4, ScriptContext.ENGINE_SCOPE);
        engine.eval("result := cross_join(ds3, ds4);");

        var result = (Dataset) engine.getContext().getAttribute("result");
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
