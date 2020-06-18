package fr.insee.vtl.engine.visitors.expression;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import static org.assertj.core.api.Assertions.assertThat;

public class ArithmeticExprTest {

    private ScriptEngine engine;

    @BeforeEach
    public void setUp() {
        engine = new ScriptEngineManager().getEngineByName("vtl");
    }

    @Test
    public void testArithmeticExpr() throws ScriptException {
        ScriptContext context = engine.getContext();
        engine.eval("mul := 2 * 3;");
        assertThat(context.getAttribute("mul")).isEqualTo(6L);

        engine.eval("mul := 1.5 * 2;");
        assertThat(context.getAttribute("mul")).isEqualTo(3.0);
        engine.eval("mul := 2 * 1.5;");
        assertThat(context.getAttribute("mul")).isEqualTo(3.0);
        engine.eval("mul := 2.0 * 1.5;");
        assertThat(context.getAttribute("mul")).isEqualTo(3.0);

        engine.eval("div := 6 / 3;");
        assertThat(context.getAttribute("div")).isEqualTo(2.0);
        engine.eval("div := 1 / 0.5;");
        assertThat(context.getAttribute("div")).isEqualTo(2.0);
        engine.eval("div := 2.0 / 1;");
        assertThat(context.getAttribute("div")).isEqualTo(2.0);
        engine.eval("div := 3.0 / 1.5;");
        assertThat(context.getAttribute("div")).isEqualTo(2.0);
    }
}
