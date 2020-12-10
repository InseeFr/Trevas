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
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class JoinFunctionsTest {
    private ScriptEngine engine;

    @BeforeEach
    public void setUp() {
        engine = new ScriptEngineManager().getEngineByName("vtl");
    }

    @Test
    public void testJoinWithAlias() throws ScriptException {

        InMemoryDataset dataset1 = new InMemoryDataset(
                List.of(
                        List.of("a", 1L, 2L),
                        List.of("b", 3L, 4L),
                        List.of("c", 5L, 6L)
                ),
                List.of(
                        new Structured.Component("name", String.class, Dataset.Role.IDENTIFIER),
                        new Structured.Component("age", Long.class, Dataset.Role.MEASURE),
                        new Structured.Component("weight", Long.class, Dataset.Role.MEASURE)
                )
        );
        InMemoryDataset dataset2 = new InMemoryDataset(
                List.of(
                        List.of(7L, "a", 8L),
                        List.of(9L, "b", 10L),
                        List.of(11L, "c", 12)
                ),
                List.of(
                        new Structured.Component("age2", Long.class, Dataset.Role.MEASURE),
                        new Structured.Component("name", String.class, Dataset.Role.IDENTIFIER),
                        new Structured.Component("weight2", Long.class, Dataset.Role.MEASURE)
                )
        );
        InMemoryDataset dataset3 = new InMemoryDataset(
                List.of(),
                List.of(
                        new Structured.Component("name", String.class, Dataset.Role.IDENTIFIER),
                        new Structured.Component("age3", Long.class, Dataset.Role.MEASURE),
                        new Structured.Component("weight3", Long.class, Dataset.Role.MEASURE)
                )
        );

        ScriptContext context = engine.getContext();
        context.getBindings(ScriptContext.ENGINE_SCOPE).put("ds1", dataset1);
        context.getBindings(ScriptContext.ENGINE_SCOPE).put("ds2", dataset2);
        context.getBindings(ScriptContext.ENGINE_SCOPE).put("ds3", dataset3);

        engine.eval("result := left_join(ds1 as dsOne, ds2, ds3)");
    }

    @Test
    public void testLeftJoin() {
        assertThatThrownBy(() -> engine.eval("result := left_join(a, b, c)"))
                .hasMessage("TODO: left_join");
    }

    @Test
    public void testLeftJoinWithDouble() throws ScriptException {
        InMemoryDataset dataset1 = new InMemoryDataset(
                List.of(
                        List.of("Hadrien", 10L),
                        List.of("Nico", 11L)
                ),
                List.of(
                        new Structured.Component("name", String.class, Dataset.Role.IDENTIFIER),
                        new Structured.Component("age", Long.class, Dataset.Role.MEASURE)
                )
        );
        InMemoryDataset dataset2 = new InMemoryDataset(
                List.of(
                        List.of("Hadrien", 20L),
                        List.of("Nico", 22L)
                ),
                List.of(
                        new Structured.Component("name", String.class, Dataset.Role.IDENTIFIER),
                        new Structured.Component("age", Long.class, Dataset.Role.MEASURE)
                )
        );

        ScriptContext context = engine.getContext();
        context.getBindings(ScriptContext.ENGINE_SCOPE).put("ds1", dataset1);
        context.getBindings(ScriptContext.ENGINE_SCOPE).put("ds2", dataset2);

        engine.eval("result := left_join(ds1, ds2)");
        assertThat(((Dataset) engine.getContext().getAttribute("result")).getDataAsMap()).containsExactlyInAnyOrder(
                Map.of("pseudo", "Hadrien", "age", 11L),
                Map.of("pseudo", "Nico", "age", 11L)
        );

        engine.eval("result := left_join(ds1, ds2 apply ds1 + ds2)");
        assertThat(((Dataset) engine.getContext().getAttribute("result")).getDataAsMap()).containsExactlyInAnyOrder(
                Map.of("pseudo", "Hadrien", "age", 30L),
                Map.of("pseudo", "Nico", "age", 30L)
        );

    }

    @Test
    public void testInnerJoin() {
        assertThatThrownBy(() -> engine.eval("result := inner_join(a, b, c)"))
                .hasMessage("TODO: inner_join");
    }

    @Test
    public void testCrossJoin() {
        assertThatThrownBy(() -> engine.eval("result := cross_join(a, b, c)"))
                .hasMessage("TODO: cross_join");
    }

    @Test
    public void testFullJoin() {
        assertThatThrownBy(() -> engine.eval("result := full_join(a, b, c)"))
                .hasMessage("TODO: full_join");
    }
}