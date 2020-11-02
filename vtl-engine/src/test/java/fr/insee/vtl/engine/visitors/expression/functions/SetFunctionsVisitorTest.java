package fr.insee.vtl.engine.visitors.expression.functions;

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

import static org.assertj.core.api.Assertions.assertThat;

public class SetFunctionsVisitorTest {

    private ScriptEngine engine;

    @BeforeEach
    public void setUp() {
        engine = new ScriptEngineManager().getEngineByName("vtl");
    }

    @Test
    public void testUnionSimple() throws ScriptException {

        InMemoryDataset dataset = new InMemoryDataset(
                List.of(
                        Map.of("name", "Hadrien", "age", 10L, "weight", 11L),
                        Map.of("name", "Nico", "age", 11L, "weight", 10L),
                        Map.of("name", "Franck", "age", 12L, "weight", 9L)
                ),
                Map.of("name", String.class, "age", Long.class, "weight", Long.class),
                Map.of("name", Dataset.Role.IDENTIFIER, "age", Dataset.Role.MEASURE, "weight", Dataset.Role.MEASURE)
        );
        ScriptContext context = engine.getContext();
        context.getBindings(ScriptContext.ENGINE_SCOPE).put("ds1", dataset);
        context.getBindings(ScriptContext.ENGINE_SCOPE).put("ds2", dataset);

        engine.eval("result := union(ds1, ds2);");
        Object result = engine.getContext().getAttribute("result");
        assertThat(result).isInstanceOf(Dataset.class);
        assertThat(((Dataset) result).getDataAsMap()).containsAll(
                dataset.getDataAsMap()
        );

    }

    @Test
    public void testUnion() throws ScriptException {

        InMemoryDataset dataset1 = new InMemoryDataset(
                List.of(
                        Map.of("name", "Hadrien", "age", 10L, "weight", 11L),
                        Map.of("name", "Nico", "age", 11L, "weight", 10L)
                ),
                Map.of("name", String.class, "age", Long.class, "weight", Long.class),
                Map.of("name", Dataset.Role.IDENTIFIER, "age", Dataset.Role.MEASURE, "weight", Dataset.Role.MEASURE)
        );
        InMemoryDataset dataset2 = new InMemoryDataset(
                List.of(
                        Map.of("name", "Hadrien", "age", 10L, "weight", 11L),
                        Map.of("name", "Franck", "age", 12L, "weight", 9L)
                ),
                Map.of("name", String.class, "age", Long.class, "weight", Long.class),
                Map.of("name", Dataset.Role.IDENTIFIER, "age", Dataset.Role.MEASURE, "weight", Dataset.Role.MEASURE)
        );
        ScriptContext context = engine.getContext();
        context.getBindings(ScriptContext.ENGINE_SCOPE).put("ds1", dataset1);
        context.getBindings(ScriptContext.ENGINE_SCOPE).put("ds2", dataset2);

        engine.eval("result := union(ds1, ds2);");
        Object result = engine.getContext().getAttribute("result");
        assertThat(result).isInstanceOf(Dataset.class);
        assertThat(((Dataset) result).getDataAsMap()).containsExactlyInAnyOrder(
                Map.of("name", "Hadrien", "age", 10L, "weight", 11L),
                Map.of("name", "Nico", "age", 11L, "weight", 10L),
                Map.of("name", "Franck", "age", 12L, "weight", 9L)
        );

    }
}