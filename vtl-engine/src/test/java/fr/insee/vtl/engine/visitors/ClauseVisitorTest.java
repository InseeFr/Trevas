package fr.insee.vtl.engine.visitors;

import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.InMemoryDataset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.util.List;
import java.util.Map;

import static fr.insee.vtl.model.Dataset.Role;
import static org.assertj.core.api.Assertions.assertThat;

public class ClauseVisitorTest {

    private ScriptEngine engine;

    @BeforeEach
    public void setUp() {
        engine = new ScriptEngineManager().getEngineByName("vtl");
    }

    @Test
    public void testFilterClause() throws ScriptException {

        InMemoryDataset dataset = new InMemoryDataset(
                List.of(
                        Map.of("name", "Hadrien", "age", 10L, "weight", 11L),
                        Map.of("name", "Nico", "age", 11L, "weight", 10L),
                        Map.of("name", "Franck", "age", 12L, "weight", 9L)
                ),
                Map.of("name", String.class, "age", Long.class, "weight", Long.class),
                Map.of("name", Role.IDENTIFIER, "age", Role.MEASURE, "weight", Role.MEASURE)
        );

        ScriptContext context = engine.getContext();
        context.setAttribute("ds1", dataset, ScriptContext.ENGINE_SCOPE);

        engine.eval("ds := ds1[filter age > 10 and age < 12];");

        assertThat(engine.getContext().getAttribute("ds")).isInstanceOf(Dataset.class);
        assertThat(((Dataset) engine.getContext().getAttribute("ds")).getDataAsMap()).isEqualTo(List.of(
                Map.of("name", "Nico", "age", 11L, "weight", 10L)
        ));

    }

    @Test
    public void testCalcClause() throws ScriptException {

        InMemoryDataset dataset = new InMemoryDataset(
                List.of(
                        Map.of("name", "Hadrien", "age", 10L, "weight", 11L),
                        Map.of("name", "Nico", "age", 11L, "weight", 10L),
                        Map.of("name", "Franck", "age", 12L, "weight", 9L)
                ),
                Map.of("name", String.class, "age", Long.class, "weight", Long.class),
                Map.of("name", Role.IDENTIFIER, "age", Role.MEASURE, "weight", Role.MEASURE)
        );

        ScriptContext context = engine.getContext();
        context.setAttribute("ds1", dataset, ScriptContext.ENGINE_SCOPE);

        engine.eval("ds := ds1[calc res := age + weight / 2];");

        assertThat(engine.getContext().getAttribute("ds")).isInstanceOf(Dataset.class);
        assertThat(((Dataset) engine.getContext().getAttribute("ds")).getDataAsMap()).containsExactly(
                Map.of("name", "Hadrien", "res", 15.5, "age", 10L, "weight", 11L),
                Map.of("name", "Nico", "res", 16.0, "age", 11L, "weight", 10L),
                Map.of("name", "Franck", "res", 16.5, "age", 12L, "weight", 9L)
        );

    }
}