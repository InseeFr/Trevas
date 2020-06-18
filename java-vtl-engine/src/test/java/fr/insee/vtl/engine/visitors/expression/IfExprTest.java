package fr.insee.vtl.engine.visitors.expression;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import static org.assertj.core.api.Assertions.assertThat;

public class IfExprTest {

    private ScriptEngine engine;

    @BeforeEach
    public void setUp() {
        engine = new ScriptEngineManager().getEngineByName("vtl");
    }

    @Test
    public void testIfExpr() throws ScriptException {
        ScriptContext context = engine.getContext();
        engine.eval("s := if true then \"true\" else \"false\";");
        assertThat(context.getAttribute("s")).isEqualTo("true");
        engine.eval("l := if false then 1 else 0;");
        assertThat(context.getAttribute("l")).isEqualTo(0L);
    }
}
