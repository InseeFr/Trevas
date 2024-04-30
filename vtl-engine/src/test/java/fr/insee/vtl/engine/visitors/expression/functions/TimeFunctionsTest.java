package fr.insee.vtl.engine.visitors.expression.functions;

import fr.insee.vtl.model.InMemoryDataset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;

import static fr.insee.vtl.model.Dataset.Component;
import static fr.insee.vtl.model.Dataset.Role;
import static org.assertj.core.api.Assertions.assertThat;

public class TimeFunctionsTest {

    private ScriptEngine engine;

    @BeforeEach
    public void setUp() {
        engine = new ScriptEngineManager().getEngineByName("vtl");
    }

    @Test
    public void testCurrentDateAtom() throws ScriptException {
        ScriptContext context = engine.getContext();
        engine.eval("a := current_date();");
        assertThat(((Instant) context.getAttribute("a"))).isNotNull();
    }

    @Test
    public void testFlowToStock() throws ScriptException {
        var ds = new InMemoryDataset(
                List.of(
                        new Component("id1", String.class, Role.IDENTIFIER),
                        new Component("id2", OffsetDateTime.class, Role.IDENTIFIER),
                        new Component("me1", Long.class, Role.MEASURE)
                ),
                List.of("A", OffsetDateTime.parse("2010-01-01T00:00:00+01:00"), 2L),
                List.of("A", OffsetDateTime.parse("2010-01-01T00:00:00+01:00"), 5L),
                List.of("A", OffsetDateTime.parse("2010-01-01T00:00:00+01:00"), -3L),
                List.of("A", OffsetDateTime.parse("2010-01-01T00:00:00+01:00"), 9L),
                List.of("A", OffsetDateTime.parse("2010-01-01T00:00:00+01:00"), 4L)
        );
        engine.put("ds", ds);
        engine.eval("r := flow_to_stock(ds);");
        assertThat(engine.get("r")).isNotNull();
    }

    @Test
    public void testStockToFlow() throws ScriptException {
        ScriptContext context = engine.getContext();
        engine.eval("a := current_date();");
        assertThat(((Instant) context.getAttribute("a"))).isNotNull();
    }
}
