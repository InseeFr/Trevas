package fr.insee.vtl.engine.visitors.expression;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import static org.assertj.core.api.Assertions.assertThat;

public class UnaryExprTest {

    private ScriptEngine engine;

    @BeforeEach
    public void setUp() {
        engine = new ScriptEngineManager().getEngineByName("vtl");
    }

    @Test
    public void testNull() throws ScriptException {
        ScriptContext context = engine.getContext();
        engine.eval("res := - null;");
        assertThat(context.getAttribute("res")).isNull();
        engine.eval("res := + null;");
        assertThat(context.getAttribute("res")).isNull();
        engine.eval("res := not null;");
        assertThat(context.getAttribute("res")).isNull();
    }

    @Test
    public void unaryExpr() throws ScriptException {
        ScriptContext context = engine.getContext();
        engine.eval("a := + 1;");
        assertThat(context.getAttribute("a")).isEqualTo(1L);
        engine.eval("a := +1.1;");
        assertThat(context.getAttribute("a")).isEqualTo(1.1D);
        engine.eval("a := - 1;");
        assertThat(context.getAttribute("a")).isEqualTo(-1L);
        engine.eval("a := -1.1;");
        assertThat(context.getAttribute("a")).isEqualTo(-1.1D);
        engine.eval("a := not true;");
        assertThat(context.getAttribute("a")).isEqualTo(false);
        engine.eval("a := not false;");
        assertThat(context.getAttribute("a")).isEqualTo(true);
    }
}
