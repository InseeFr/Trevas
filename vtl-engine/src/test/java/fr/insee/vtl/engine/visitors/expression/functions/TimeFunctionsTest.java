package fr.insee.vtl.engine.visitors.expression.functions;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.time.Instant;

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
}
