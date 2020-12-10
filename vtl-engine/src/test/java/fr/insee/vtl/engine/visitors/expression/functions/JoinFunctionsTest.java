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
    public void testDuplicateNameNotSupportedYet() {
        InMemoryDataset dataset1 = new InMemoryDataset(
                List.of(),
                List.of(
                        new Structured.Component("name", String.class, Role.IDENTIFIER),
                        new Structured.Component("age", Long.class, Role.MEASURE),
                        new Structured.Component("weight", Long.class, Role.MEASURE)
                )
        );
        InMemoryDataset dataset2 = new InMemoryDataset(
                List.of(),
                List.of(
                        new Structured.Component("name", String.class, Role.IDENTIFIER),
                        new Structured.Component("age", Long.class, Role.MEASURE),
                        new Structured.Component("weight", Long.class, Role.MEASURE)
                )
        );

        engine.getContext().setAttribute("ds1", dataset1, ScriptContext.ENGINE_SCOPE);
        engine.getContext().setAttribute("ds2", dataset2, ScriptContext.ENGINE_SCOPE);
        assertThatThrownBy(() -> engine.eval("result := left_join(ds1 as dsOne, ds2);"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("duplicate");
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
        engine.eval("result := left_join(ds1, ds2 using id1);");

        var result = (Dataset) engine.getContext().getAttribute("result");
        assertThat(result.getDataAsList()).containsExactlyInAnyOrder(
                Arrays.asList("a", 1L, 2L, 9L, 10L),
                Arrays.asList("b", 3L, 4L, 11L, 12L),
                Arrays.asList("c", 5L, 6L, 12L, 13L),
                Arrays.asList("c", 5L, 6L, 14L, 15L),
                Arrays.asList("d", 7L, 8L, null, null)
        );
    }

    @Test
    public void testJoinWithAlias() throws ScriptException {

        InMemoryDataset dataset1 = new InMemoryDataset(
                List.of(
                        List.of("a", 1L, 2L),
                        List.of("b", 3L, 4L),
                        List.of("c", 5L, 6L),
                        List.of("d", 7L, 8L)
                ),
                List.of(
                        new Structured.Component("name", String.class, Role.IDENTIFIER),
                        new Structured.Component("age", Long.class, Role.MEASURE),
                        new Structured.Component("weight", Long.class, Role.MEASURE)
                )
        );
        InMemoryDataset dataset2 = new InMemoryDataset(
                List.of(
                        List.of(9L, "a", 10L),
                        List.of(11L, "b", 12L),
                        List.of(12L, "c", 13L),
                        List.of(14L, "c", 15L)
                ),
                List.of(
                        new Structured.Component("age2", Long.class, Role.MEASURE),
                        new Structured.Component("name", String.class, Role.IDENTIFIER),
                        new Structured.Component("weight2", Long.class, Role.MEASURE)
                )
        );

        InMemoryDataset dataset3 = new InMemoryDataset(
                List.of(
                        List.of(16L, "a", 17L),
                        List.of(18L, "b", 19L),
                        List.of(20L, "c", 21L),
                        List.of(22L, "c", 23L)
                ),
                List.of(
                        new Structured.Component("age3", Long.class, Role.MEASURE),
                        new Structured.Component("name", String.class, Role.IDENTIFIER),
                        new Structured.Component("weight3", Long.class, Role.MEASURE)
                )
        );

        ScriptContext context = engine.getContext();
        context.getBindings(ScriptContext.ENGINE_SCOPE).put("ds1", dataset1);
        context.getBindings(ScriptContext.ENGINE_SCOPE).put("ds2", dataset2);
        context.getBindings(ScriptContext.ENGINE_SCOPE).put("ds3", dataset3);

        engine.eval("result := left_join(ds1 as dsOne, ds2, ds3);");

        var result = (Dataset) context.getAttribute("result");
        assertThat(result.getDataAsList()).containsExactlyInAnyOrder(
                Arrays.asList("a", 1L, 2L, 9L, 10L),
                Arrays.asList("b", 3L, 4L, 11L, 12L),
                Arrays.asList("c", 5L, 6L, 12L, 13L),
                Arrays.asList("c", 5L, 6L, 14L, 15L),
                Arrays.asList("d", 7L, 8L, null, null)
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
                Map.of("pseudo", "Hadrien", "age", 11L),
                Map.of("pseudo", "Nico", "age", 11L)
        );

        engine.eval("result := left_join(ds1, ds2 apply ds1 + ds2);");
        assertThat(((Dataset) engine.getContext().getAttribute("result")).getDataAsMap()).containsExactlyInAnyOrder(
                Map.of("pseudo", "Hadrien", "age", 30L),
                Map.of("pseudo", "Nico", "age", 30L)
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