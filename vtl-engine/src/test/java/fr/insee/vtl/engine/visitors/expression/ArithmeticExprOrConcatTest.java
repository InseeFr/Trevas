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
    public void testNull() throws ScriptException {
        ScriptContext context = engine.getContext();
        // Plus
        engine.eval("res := 1 + null;");
        assertThat((Long) context.getAttribute("res")).isNull();
        engine.eval("res := null + 1;");
        assertThat((Long) context.getAttribute("res")).isNull();
        // Minus
        engine.eval("res := 1 - null;");
        assertThat((Long) context.getAttribute("res")).isNull();
        engine.eval("res := null - 1;");
        assertThat((Long) context.getAttribute("res")).isNull();
        // Concat
        engine.eval("res := \"\" || null;");
        assertThat((Boolean) context.getAttribute("res")).isNull();
        engine.eval("res := null || \"\";");
        assertThat((Boolean) context.getAttribute("res")).isNull();
    }

    @Test
    public void testArithmeticExprOrConcat() throws ScriptException {
        ScriptContext context = engine.getContext();
        engine.eval("plus := 2 + 3; minus := 3 - 2; concat := \"3\" || \"ok\";");
        assertThat(context.getAttribute("plus")).isEqualTo(5L);
        assertThat(context.getAttribute("minus")).isEqualTo(1L);
        assertThat(context.getAttribute("concat")).isEqualTo("3ok");
        engine.eval("plus := 2 + 3.0; minus := 3.0 - 2;");
        assertThat(context.getAttribute("plus")).isEqualTo(5.0);
        assertThat(context.getAttribute("minus")).isEqualTo(1.0);
        engine.eval("plus := 2.0 + 3; minus := 3 - 2.0;");
        assertThat(context.getAttribute("plus")).isEqualTo(5.0);
        assertThat(context.getAttribute("minus")).isEqualTo(1.0);
    }
}
