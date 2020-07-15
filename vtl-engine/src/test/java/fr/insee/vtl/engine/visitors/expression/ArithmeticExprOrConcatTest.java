package fr.insee.vtl.engine.visitors.expression;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import static org.assertj.core.api.Assertions.assertThat;

public class ArithmeticExprOrConcatTest {

    private ScriptEngine engine;

    @BeforeEach
    public void setUp() {
        engine = new ScriptEngineManager().getEngineByName("vtl");
    }

    @Test
    public void testArithmeticExprOrConcat() throws ScriptException {
        ScriptContext context = engine.getContext();
        engine.eval("plus := 2 + 3; minus := 3 - 2; concat := 3 || 2;");
        assertThat(context.getAttribute("plus")).isEqualTo(5L);
        assertThat(context.getAttribute("minus")).isEqualTo(1L);
        assertThat(context.getAttribute("concat")).isEqualTo(32L);
    }
}
