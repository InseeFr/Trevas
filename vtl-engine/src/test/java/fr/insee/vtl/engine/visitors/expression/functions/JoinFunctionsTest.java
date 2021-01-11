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
import java.util.List;
import java.util.Map;

import static fr.insee.vtl.model.Dataset.Role;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class JoinFunctionsTest {
    private ScriptEngine engine;

    @BeforeEach
    public void setUp() {
        engine = new ScriptEngineManager().getEngineByName("vtl");
    }

    @Test
    public void testLeftJoinWithUsing() throws ScriptException {
        var ds1 = new InMemoryDataset(
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

        var ds2 = new InMemoryDataset(
                List.of(
                        List.of("a", 1L, 7L),
                        List.of("a", 2L, 8L),
                        List.of("b", 1L, 9L),
                        List.of("b", 2L, 10L),
                        List.of("c", 3L, 11L),
                        List.of("c", 4L, 12L)
                ),
                List.of(
                        new Structured.Component("id1", String.class, Role.IDENTIFIER),
                        new Structured.Component("id2", Long.class, Role.IDENTIFIER),
                        new Structured.Component("m2", Long.class, Role.MEASURE)
                )
        );

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
                 Arrays.asList("c", 1L, 5L, 3L, 11L),
                 Arrays.asList("c", 1L, 5L, 4L, 12L),
                 Arrays.asList("c", 2L, 6L, 3L, 11L),
                 Arrays.asList("c", 2L, 6L, 4L, 12L)
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
                Map.of("name", "Hadrien", "ds1#age",10L, "ds2#age",20L),
                Map.of("name", "Nico", "ds1#age",11L, "ds2#age",22L)
        );

    }

    @Test
    public void testInnerJoin() {
        assertThatThrownBy(() -> engine.eval("result := inner_join(a, b, c);"))
                .hasMessage("TODO: inner_join");
    }

    @Test
    public void testCrossJoin() {
        assertThatThrownBy(() -> engine.eval("result := cross_join(a, b, c);"))
                .hasMessage("TODO: cross_join");
    }

    @Test
    public void testFullJoin() {
        assertThatThrownBy(() -> engine.eval("result := full_join(a, b, c);"))
                .hasMessage("TODO: full_join");
    }
}